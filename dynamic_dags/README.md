---
post_title: Creating Dynamic DAGs
nav_title: Creating Dynamic DAGs
---

# Standard Dynamic Dag
This DAG is meant to represent the various concepts of creating a dynamic DAG file. At a high-level this DAG file leverages an array of DAG level settings, loops through the array, creates DAG objects and adds them to the global DagBag.

## Context and Assumptions
### Context
This DAG file provides an example of dynamically creating multiple DAGs each performing a similar set of work for many customers. A real world example may be creating many DAGs (one for each customer) that move clickstream data from each customers unique source to a singular location.
### Assumptions
* Each Customer Will Have a Unique Source Connection
* Actual Workflow will be represented by DummyOperators

## Concepts and Features
### Settings Array
Depending on who will be controlling the creation of dynamic DAGs in your business, this variable can sit directly in the DAG file itself or it can pulled from [Airflow Variables](https://pythonhosted.org/airflow/concepts.html#variables). If done this way, it may be easier for a business user to modify the JSON variable themselves in order to create a new dynamic DAG.

In our example settings array we have four properties on each object.
* Customer Name
    * A human readable customer name.
* Customer Id
    * A valid string Id. This will be used for the creation of a unique DAG name and connection names.
* Email
    * The email array is an example of an override setting we are passing into DAG creation. In this example it is an array of emails that is used to override the ```default_args``` setting object. In order to override the settings of ```default_args``` the property names must be identical in order for ovverride to work.

    * The actual overriding of settings takes place in the following code but anything similar will work.
    ```replaced_args = {k: default_args[k] if customer.get(k, None) is None else customer[k] for k in default_args}```
* Enabled
    * **TODO: Show programmatic pausing of a DAG using this property.**

### Create DAG Function
In here we handle the overriding of ```default_args```, the naming of the unique DAG and the creation of the DAG object. Which is handed off to the global DAGBag in the settings array loop.

### Creating A New DAG
Using the above example, creating a new DAG is as easy as adding to the Settings Array. Next time that the DAG runs it will pick up the new object and create the new DAG.

## Gotchas
### New Connections
We recommend creating your connection dependencies before creating a new DAG. Doing otherwise will result in each created DAG (that is missing a connection) to fail. You can also create the connection programmatically by passing in connection information through the Settings Array that is used to create the DAGs themselves. If done this way, please consider encrpytion on any sensitive information being passed through the array.

### Deleting A DAG
At time of writing [deleting of a DAG](https://github.com/apache/incubator-airflow/pull/2199) is still an open issue with the Airflow repo. Deleting a DAG is not recommended until officially supported by Airflow. Until then the recommendation is to pause DAG.