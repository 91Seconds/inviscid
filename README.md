# Inviscid
## Design Problems
* One of the essential features of this is not re rerunning tasks that don't need to be rerun.  A task does not need to be rerun if it is a pure function, and it has already been run and outputs cached.  If we keep track of which inputs a task receives then we can check only those parameters.  One problem is that we need to know these parameters before we run the function in order to decide whether we have to run the parameters.  There are a few solutions to this problem, some of which are not very nice.
    * We give a set of config keys as a function attribute.
    * We make the function return a set of config keys and a callback to the function that we actually wanted.
  
  Both of these work fine for the function executor but are clumsy to setup for the user defining the function.
    * The function could ingest all its config through its parameters, and we can inspect the function's signature prior to execution to determine what config it needs.
  
  This is the only way I've found that makes it nice at the function definition stage but we don't want users to be responsible for passing config to their functions because the config is not known until runtime and because we want to make it easy to do things the right way and if the decorator does the config passing it will discourage ad hoc parameter overrides.
