# Inviscid
## Design Problems
* One of the essential features of this is not re reunning tasks that don't need to be rerun.

* What arguments does a task need to receive?  It needs objects from upstream tasks and it needs config from the parent dag.  When we create tasks with the decorator api we call the decorated object with arguments from downstream tasks.  The way it's setup now, this precludes us from having the dag pass config to the decorated function directly by the functions arguments.  It would be really good if the config could be passed to the user defined function by the functions's arguments because then we can analyse the function's signature to find it's domain before running it.  This allows us to determine if the function needs to be run or not (if the config being used and the src and previous steps are the same then it does not need to run again)

