#define _GNU_SOURCE
#include "minispark.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <sched.h>
#include <unistd.h>

// structure for node for list
typedef struct LNode
{
  void *data;
  struct LNode *next;
} LNode;

// structure for list
typedef struct List
{
  int size;
  int numpartitions;
  LNode *head;
  LNode *tail;
} List;

// structure for node for queue
typedef struct Node
{
  Task *task;
  struct Node *next;
} Node;

// structure for queue
typedef struct Queue
{
  int size;
  Node *front, *end;
  pthread_mutex_t lock;
  pthread_cond_t empty; // signals when queue is empty or not
} Queue;

typedef struct TPool
{
  int numthreads; // worker threads
  int exit_flag;  // flag to exit worker threads
  Queue queue;
  pthread_t *threads;
  int num_working_threads; // workers in-progress
  pthread_cond_t working;
} TPool;

// structure for metric node for metric queue
typedef struct MNode
{
  TaskMetric *metric;
  struct MNode *next;
} MNode;

// structure for metric queue
typedef struct MQueue
{
  int size;
  MNode *front, *end;
  pthread_mutex_t lock;
  pthread_cond_t empty; // signals when queue is empty or not
} MQueue;

static TPool *thread_pool = NULL;
static MQueue mqueue;      // metric queue
static pthread_t mthread;  // metric thread
static int mexit_flag = 0; // checks when we're done with metrics

// initialize list
List *list_init(int numpartitions)
{
  List *list = (List *)malloc(sizeof(List));
  if (list == NULL)
    exit(1);

  list->size = 0;
  list->numpartitions = numpartitions;
  list->head = NULL;
  list->tail = NULL;

  return list;
}

// append to end of list
void list_add_elem(List *list, void *data)
{
  if (list == NULL || data == NULL)
    return;

  LNode *node = (LNode *)malloc(sizeof(LNode));
  if (node == NULL)
    exit(1);

  node->data = data;
  node->next = NULL;

  if (list->size == 0) // list is empty
  {
    list->head = node;
    list->tail = node;
  }
  else // add to end of list
  {
    list->tail->next = node;
    list->tail = node;
  }

  list->size++; // update size
}

// get data at certain index
void *list_get(List *list, int index)
{
  if (list == NULL || index < 0 || index >= list->size)
    return NULL;

  LNode *node = list->head;
  for (int i = 0; i < index; i++) // iterate until index
  {
    if (node == NULL)
      return NULL;
    node = node->next;
  }

  return node->data;
}

// frees list and data in list if there is any
void list_free(List *list)
{
  if (list == NULL)
    return;

  LNode *node = list->head;
  while (node != NULL)
  {
    LNode *next = node->next;
    if (node->data != NULL)
      free(node->data); // frees data
    free(node);         // frees actual node
    node = next;
  }

  free(list); // frees list
}

// initializes the queue
void queue_init(Queue *queue)
{
  queue->size = 0;
  queue->front = NULL;
  queue->end = NULL;
  pthread_mutex_init(&queue->lock, NULL);
  pthread_cond_init(&queue->empty, NULL);
}

// frees the queue
void queue_free(Queue *queue)
{
  if (queue == NULL)
    return;

  Node *node = queue->front;
  while (node != NULL) // free all tasks in queue
  {
    Node *next = node->next;
    if (node->task != NULL)
      free(node->task); // free task
    free(node);         // free actual node
    node = next;
  }

  queue->size = 0;
  queue->front = NULL;
  queue->end = NULL;
  pthread_mutex_destroy(&queue->lock); // destroys mutex lock from init
  pthread_cond_destroy(&queue->empty); // destroys condition variable empty
}

// enqueue a task to the queue
void enqueue(Queue *queue, Task *task)
{
  if (queue == NULL || task == NULL)
    return;

  Node *node = (Node *)malloc(sizeof(Node));
  if (node == NULL)
    exit(1);

  node->task = task;
  node->next = NULL;

  pthread_mutex_lock(&queue->lock);
  if (queue->size == 0) // if queue is empty
  {
    queue->front = node;
    queue->end = node;
  }
  else // add to end of queue
  {
    queue->end->next = node;
    queue->end = node;
  }

  queue->size++;                      // update size
  pthread_cond_signal(&queue->empty); // wakes up thread to go get a task
  pthread_mutex_unlock(&queue->lock);
}

// dequeues task from queue and returns it
Task *dequeue(Queue *queue)
{
  if (queue == NULL)
    return NULL;

  pthread_mutex_lock(&queue->lock);
  while (queue->size == 0)
  {
    if (thread_pool && thread_pool->exit_flag == 1) // if thread pool stop accepting tasks
    {
      pthread_mutex_unlock(&queue->lock);
      return NULL;
    }
    pthread_cond_wait(&queue->empty, &queue->lock); // wait till task comes
  }

  Node *node = queue->front;
  Task *task = node->task;   // task at front of queue
  queue->front = node->next; // update front of queue
  queue->size--;             // update size

  if (queue->size == 0) // if there are no more remaining nodes in queue
  {
    queue->front = NULL; // update front of queue
    queue->end = NULL;   // update end of queue
  }

  pthread_mutex_unlock(&queue->lock);
  return task;
}

void thread_pool_submit(Task *task)
{
  enqueue(&thread_pool->queue, task); // add new task
}

// complete an arbitrary task
void complete_task(Task *task)
{
  // rdd to complete the task on and its partition number
  RDD *rdd = task->rdd;
  int pnum = task->pnum;

  // match transformation
  switch (rdd->trans)
  {
  case MAP:
    Mapper mapper = (Mapper)rdd->fn;

    // simplified case for filebacked dependency
    if (rdd->dependencies[0]->trans == FILE_BACKED)
    {
      // grab data until there is none
      void *data;
      void *fp = list_get(rdd->dependencies[0]->partitions, pnum);
      List *target_list = list_get(rdd->partitions, pnum);
      while ((data = mapper(fp)) != NULL)
      {
        list_add_elem(target_list, data); // add the data
      }
    }

    else
    {
      // target and dependency partition lists
      List *target_list = list_get(rdd->partitions, pnum);
      List *dep_list = list_get(rdd->dependencies[0]->partitions, pnum);

      // iterate through dependency partition and map it over
      LNode *dep_node = dep_list->head;
      while (dep_node != NULL)
      {
        list_add_elem(target_list, mapper(dep_node->data));
        dep_node = dep_node->next;
      }
    }
    break;

  case FILTER:
    Filter filterer = (Filter)rdd->fn;

    // target and dependency partition lists
    List *filter_target_list = list_get(rdd->partitions, pnum);
    List *dep_list = list_get(rdd->dependencies[0]->partitions, pnum);

    // iterate through dependency partition and filter
    LNode *dep_node = dep_list->head;
    while (dep_node != NULL)
    {
      if (filterer(dep_node->data, rdd->ctx) != 0)
        list_add_elem(filter_target_list, dep_node->data);
      dep_node = dep_node->next;
    }
    break;

  case JOIN:
    Joiner joiner = (Joiner)rdd->fn;

    // target and dependency partition lists
    List *join_target_list = list_get(rdd->partitions, pnum);
    List *dep1_list = list_get(rdd->dependencies[0]->partitions, pnum);
    List *dep2_list = list_get(rdd->dependencies[1]->partitions, pnum);

    // n^2 comparisons
    LNode *dep1_node = dep1_list->head;
    while (dep1_node != NULL)
    {
      LNode *dep2_node = dep2_list->head;
      while (dep2_node != NULL)
      {
        // add newly created data
        list_add_elem(join_target_list, joiner(dep1_node->data, dep2_node->data, rdd->ctx));
        dep2_node = dep2_node->next;
      }
      dep1_node = dep1_node->next;
    }
    break;

  case PARTITIONBY:
    Partitioner partitioner = (Partitioner)rdd->fn;

    // iterate through dependency elements and repartition
    LNode *outer_node = rdd->dependencies[0]->partitions->head;
    while (outer_node != NULL)
    {
      LNode *inner_node = ((List *)outer_node->data)->head;
      while (inner_node != NULL)
      {
        // calculate new partition number and put the data there
        int new_pnum = partitioner(inner_node->data, rdd->numpartitions, rdd->ctx);
        list_add_elem(list_get(rdd->partitions, new_pnum), inner_node->data);
        inner_node = inner_node->next;
      }
      outer_node = outer_node->next;
    }
    break;

  case FILE_BACKED:
    break; // do nothing
  }
}

// thread function
void *worker_func(void *arg)
{
  // thread loop
  while (1)
  {
    // grab new task, sleeping if no tasks are available (dequeue handles locking and signaling)
    Task *task = dequeue(&thread_pool->queue);
    if (task == NULL)
      break;

    struct timespec created, scheduled, end;
    clock_gettime(CLOCK_MONOTONIC, &created);
    clock_gettime(CLOCK_MONOTONIC, &scheduled); // time before task

    complete_task(task); // do the task

    clock_gettime(CLOCK_MONOTONIC, &end);    // time after task
    menqueue(task, created, scheduled, end); // add to metric queue

    RDD *rdd = task->rdd; // rdd in the task

    // notify dependents that this task finished
    LNode *dependents_node = rdd->dependents->head;
    while (dependents_node != NULL)
    {
      pthread_mutex_lock(&((RDD *)dependents_node->data)->task_lock);

      // schedule dependents if they can be scheduled
      if (--((RDD *)dependents_node->data)->dependent_tasks_left == 0)
      {
        if (((RDD *)dependents_node->data)->trans == PARTITIONBY || ((RDD *)dependents_node->data)->trans == FILE_BACKED)
        {
          // one task
          Task *new_task = malloc(sizeof(Task));
          new_task->pnum = 0;
          new_task->rdd = ((RDD *)dependents_node->data);

          pthread_mutex_lock(&thread_pool->queue.lock);
          thread_pool->num_working_threads++;
          pthread_mutex_unlock(&thread_pool->queue.lock);
          thread_pool_submit(new_task);
        }
        else
        {
          // task per partition
          for (int i = 0; i < ((RDD *)dependents_node->data)->numpartitions; i++)
          {
            Task *new_task = malloc(sizeof(Task));
            new_task->pnum = i;
            new_task->rdd = ((RDD *)dependents_node->data);

            pthread_mutex_lock(&thread_pool->queue.lock);
            thread_pool->num_working_threads++;
            pthread_mutex_unlock(&thread_pool->queue.lock);
            thread_pool_submit(new_task);
          }
        }
      }

      pthread_mutex_unlock(&((RDD *)dependents_node->data)->task_lock);
      dependents_node = dependents_node->next;
    }

    pthread_mutex_lock(&thread_pool->queue.lock);
    thread_pool->num_working_threads--;
    if (thread_pool->queue.size == 0 && thread_pool->num_working_threads == 0)
    {
      pthread_cond_signal(&thread_pool->working);
    }
    pthread_mutex_unlock(&thread_pool->queue.lock);
  }

  return NULL;
}

void thread_pool_init(int numthreads)
{
  // allocate thread pool
  thread_pool = malloc(sizeof(TPool));
  if (thread_pool == NULL)
    exit(1);

  // initialize the queue
  queue_init(&thread_pool->queue);

  // initialize other thread pool members
  thread_pool->numthreads = numthreads;
  thread_pool->exit_flag = 0;
  pthread_cond_init(&thread_pool->working, NULL);
  thread_pool->threads = malloc(sizeof(pthread_t) * numthreads);
  if (thread_pool->threads == NULL)
    exit(1);

  // create workers
  for (int i = 0; i < numthreads; i++)
  {
    if (pthread_create(&thread_pool->threads[i], NULL, worker_func, NULL))
      exit(1);
  }
}

void thread_pool_destroy()
{
  // signal an exit and wake everyone up so that they can exit
  pthread_mutex_lock(&thread_pool->queue.lock);
  thread_pool->exit_flag = 1;
  pthread_cond_broadcast(&thread_pool->queue.empty);
  pthread_mutex_unlock(&thread_pool->queue.lock);

  for (int i = 0; i < thread_pool->numthreads; i++)
  {
    pthread_join(thread_pool->threads[i], NULL);
  }

  free(thread_pool->threads);                  // free threads
  pthread_cond_destroy(&thread_pool->working); // kill thread pool lock and condition
  queue_free(&thread_pool->queue);             // clean up queue
}

void thread_pool_wait()
{
  pthread_mutex_lock(&thread_pool->queue.lock);
  while (thread_pool->queue.size != 0 || thread_pool->num_working_threads != 0)
  {
    pthread_cond_wait(&thread_pool->working, &thread_pool->queue.lock);
  }
  pthread_mutex_unlock(&thread_pool->queue.lock);
}

// handles the metric log
void *mlog(void *arg)
{
  FILE *fp = fopen("metrics.log", "w");
  if (fp == NULL)
    exit(1); // file can't be opened

  while (1)
  {
    pthread_mutex_lock(&mqueue.lock);
    while (mqueue.size == 0) // empty
    {
      if (mexit_flag != 0)
      {
        pthread_mutex_unlock(&mqueue.lock);
        return NULL;
      }
      pthread_cond_wait(&mqueue.empty, &mqueue.lock);
    }

    MNode *node = mqueue.front;
    mqueue.front = node->next; // remove front node
    mqueue.size--;

    if (mqueue.front == NULL)
      mqueue.end = NULL; // update end

    pthread_mutex_unlock(&mqueue.lock);

    if (node != NULL && node->metric != NULL)
    {
      print_formatted_metric(node->metric, fp); // print using given format
      free(node->metric);
    }
    free(node);
  }

  fclose(fp);
  return NULL;
}

// enqueue to metric queue
void menqueue(Task *task, struct timespec created, struct timespec scheduled, struct timespec end)
{
  TaskMetric *metric = malloc(sizeof(TaskMetric));
  if (metric == NULL)
    return;

  metric->created = created;
  metric->scheduled = scheduled;
  metric->duration = TIME_DIFF_MICROS(scheduled, end);
  metric->rdd = task->rdd;
  metric->pnum = task->pnum;

  MNode *node = malloc(sizeof(MNode));
  if (node == NULL)
    exit(1);

  node->metric = metric;
  node->next = NULL;

  pthread_mutex_lock(&mqueue.lock);
  if (mqueue.size == 0) // if queue is empty
  {
    mqueue.front = node;
    mqueue.end = node;
  }
  else // add to end of queue
  {
    mqueue.end->next = node;
    mqueue.end = node;
  }

  mqueue.size++;                      // update size
  pthread_cond_signal(&mqueue.empty); // wake up thread
  pthread_mutex_unlock(&mqueue.lock);
}

// Working with metrics...
// Recording the current time in a `struct timespec`:
//    clock_gettime(CLOCK_MONOTONIC, &metric->created);
// Getting the elapsed time in microseconds between two timespecs:
//    duration = TIME_DIFF_MICROS(metric->created, metric->scheduled);
// Use `print_formatted_metric(...)` to write a metric to the logfile.
void print_formatted_metric(TaskMetric *metric, FILE *fp)
{
  fprintf(fp, "RDD %p Part %d Trans %d -- creation %10jd.%06ld, scheduled %10jd.%06ld, execution (usec) %ld\n",
          metric->rdd, metric->pnum, metric->rdd->trans,
          metric->created.tv_sec, metric->created.tv_nsec / 1000,
          metric->scheduled.tv_sec, metric->scheduled.tv_nsec / 1000,
          metric->duration);
}

int max(int a, int b)
{
  return a > b ? a : b;
}

RDD *create_rdd(int numdeps, Transform t, void *fn, ...)
{
  RDD *rdd = malloc(sizeof(RDD));
  if (rdd == NULL)
  {
    printf("error mallocing new rdd\n");
    exit(1);
  }

  va_list args;
  va_start(args, fn);

  int maxpartitions = 0;
  for (int i = 0; i < numdeps; i++)
  {
    RDD *dep = va_arg(args, RDD *);
    rdd->dependencies[i] = dep;
    maxpartitions = max(maxpartitions, dep->partitions->size);
  }
  va_end(args);

  rdd->numdependencies = numdeps;
  rdd->trans = t;
  rdd->fn = fn;
  rdd->partitions = NULL;
  return rdd;
}

/* RDD constructors */
RDD *map(RDD *dep, Mapper fn)
{
  RDD *rdd = create_rdd(1, MAP, fn, dep);

  // set partition count
  rdd->partitions = list_init(dep->numpartitions);
  rdd->numpartitions = dep->numpartitions;

  // fill dummy lists
  for (int i = 0; i < rdd->numpartitions; i++)
  {
    list_add_elem(rdd->partitions, list_init(0));
  }

  // create lock and set dep task amount
  pthread_mutex_init(&rdd->task_lock, NULL);
  if (dep->trans == FILE_BACKED || dep->trans == PARTITIONBY)
  {
    rdd->dependent_tasks_left = 1;
  }
  else
  {
    rdd->dependent_tasks_left = dep->numpartitions;
  }

  rdd->dependents = list_init(0); // initialize this dependent list

  // add this RDD to the dependents of the dependency
  list_add_elem(dep->dependents, rdd);
  return rdd;
}

RDD *filter(RDD *dep, Filter fn, void *ctx)
{
  RDD *rdd = create_rdd(1, FILTER, fn, dep);
  rdd->ctx = ctx;

  // set partition count
  rdd->partitions = list_init(dep->numpartitions);
  rdd->numpartitions = dep->numpartitions;

  // fill dummy lists
  for (int i = 0; i < rdd->numpartitions; i++)
  {
    list_add_elem(rdd->partitions, list_init(0));
  }

  // create lock and set dep task amount
  pthread_mutex_init(&rdd->task_lock, NULL);
  if (dep->trans == FILE_BACKED || dep->trans == PARTITIONBY)
  {
    rdd->dependent_tasks_left = 1;
  }
  else
  {
    rdd->dependent_tasks_left = dep->numpartitions;
  }

  rdd->dependents = list_init(0); // initialize this dependent list

  // add this RDD to the dependents of the dependency
  list_add_elem(dep->dependents, rdd);
  return rdd;
}

RDD *partitionBy(RDD *dep, Partitioner fn, int numpartitions, void *ctx)
{
  RDD *rdd = create_rdd(1, PARTITIONBY, fn, dep);
  rdd->partitions = list_init(numpartitions);
  rdd->numpartitions = numpartitions;
  rdd->ctx = ctx;

  // fill dummy lists
  for (int i = 0; i < rdd->numpartitions; i++)
  {
    list_add_elem(rdd->partitions, list_init(0));
  }

  // create lock and set dep task amount
  pthread_mutex_init(&rdd->task_lock, NULL);
  if (dep->trans == FILE_BACKED || dep->trans == PARTITIONBY)
  {
    rdd->dependent_tasks_left = 1;
  }
  else
  {
    rdd->dependent_tasks_left = dep->numpartitions;
  }

  rdd->dependents = list_init(0); // initialize this dependent list

  // add this RDD to the dependents of the dependency
  list_add_elem(dep->dependents, rdd);
  return rdd;
}

RDD *join(RDD *dep1, RDD *dep2, Joiner fn, void *ctx)
{
  RDD *rdd = create_rdd(2, JOIN, fn, dep1, dep2);
  rdd->ctx = ctx;

  // set partition count
  rdd->partitions = list_init(dep1->numpartitions);
  rdd->numpartitions = dep1->numpartitions;

  // fill dummy lists
  for (int i = 0; i < rdd->numpartitions; i++)
  {
    list_add_elem(rdd->partitions, list_init(0));
  }

  // create lock and set dep task amount
  pthread_mutex_init(&rdd->task_lock, NULL);
  if (dep1->trans == PARTITIONBY)
  {
    rdd->dependent_tasks_left = 2;
  }
  else
  { // this case should never happen
    rdd->dependent_tasks_left = dep1->numpartitions * 2;
  }

  rdd->dependents = list_init(0); // initialize this dependent list

  // add this RDD to the dependents of the dependencies
  list_add_elem(dep1->dependents, rdd);
  list_add_elem(dep2->dependents, rdd);
  return rdd;
}

/* A special mapper */
void *identity(void *arg)
{
  return arg;
}

/* Special RDD constructor.
 * By convention, this is how we read from input files. */
RDD *RDDFromFiles(char **filenames, int numfiles)
{
  RDD *rdd = malloc(sizeof(RDD));
  rdd->partitions = list_init(numfiles);
  rdd->numpartitions = numfiles;

  for (int i = 0; i < numfiles; i++)
  {
    FILE *fp = fopen(filenames[i], "r");
    if (fp == NULL)
    {
      perror("fopen");
      exit(1);
    }
    list_add_elem(rdd->partitions, fp);
  }

  rdd->numdependencies = 0;
  rdd->trans = FILE_BACKED;
  rdd->fn = (void *)identity;
  rdd->dependent_tasks_left = 0;  // no dependencies, can be run immediately
  rdd->dependents = list_init(0); // initialize this dependent list
  return rdd;
}

void traversal_execute(RDD *rdd)
{
  // add this RDD to the work queue if its a leaf
  if (rdd->trans == FILE_BACKED)
  {
    // only one task
    Task *new_task = malloc(sizeof(Task));
    new_task->pnum = 0;
    new_task->rdd = rdd;

    pthread_mutex_lock(&thread_pool->queue.lock);
    thread_pool->num_working_threads++;
    pthread_mutex_unlock(&thread_pool->queue.lock);

    thread_pool_submit(new_task);
  }

  // recursively traverse DAG
  for (int i = 0; i < rdd->numdependencies; i++)
  {
    traversal_execute(rdd->dependencies[i]);
  }
}

void execute(RDD *rdd)
{
  traversal_execute(rdd);
  return;
}

void MS_Run()
{
  // get the number of CPU cores
  cpu_set_t set;
  CPU_ZERO(&set);

  if (sched_getaffinity(0, sizeof(set), &set) == -1)
  {
    perror("sched_getaffinity");
    exit(1);
  }
  int num_workers = CPU_COUNT(&set) - 1;

  // initialize pool
  thread_pool_init(num_workers);

  // metric initialization
  mqueue.front = NULL;
  mqueue.end = NULL;
  mqueue.size = 0;
  pthread_mutex_init(&mqueue.lock, NULL);
  pthread_cond_init(&mqueue.empty, NULL);

  if (pthread_create(&mthread, NULL, mlog, NULL))
    exit(1); // can't create thread
}

void MS_TearDown()
{
  // destroy the pool, deallocating pool allocations
  thread_pool_destroy();

  // metrics
  pthread_mutex_lock(&mqueue.lock);
  mexit_flag = 1;
  pthread_cond_signal(&mqueue.empty);
  pthread_mutex_unlock(&mqueue.lock);

  pthread_join(mthread, NULL); // wait for thread
  pthread_mutex_destroy(&mqueue.lock);
  pthread_cond_destroy(&mqueue.empty);
}

int count(RDD *rdd)
{
  execute(rdd);
  thread_pool_wait(); // wait for completion of work
  int count = 0;

  // iterate through linked lists and count elements
  LNode *outer_node = rdd->partitions->head;
  while (outer_node != NULL)
  {
    LNode *inner_node = ((List *)outer_node->data)->head;
    while (inner_node != NULL)
    {
      count++;
      inner_node = inner_node->next;
    }
    outer_node = outer_node->next;
  }
  return count;
}

void print(RDD *rdd, Printer p)
{
  execute(rdd);
  thread_pool_wait(); // wait for completion of work

  // iterate through linked lists and print all
  LNode *outer_node = rdd->partitions->head;
  while (outer_node != NULL)
  {
    LNode *inner_node = ((List *)outer_node->data)->head;
    while (inner_node != NULL)
    {
      p(inner_node->data);
      inner_node = inner_node->next;
    }
    outer_node = outer_node->next;
  }
}