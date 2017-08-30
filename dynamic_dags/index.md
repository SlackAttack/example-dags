---
post_title: Creating Dynamic DAGs
nav_title: Creating Dynamic DAGs
---

# Standard Dynamic Dag
This DAG is meant to represent the various concepts of creating a dynamic DAG file. At a high-level this DAG file leverages an array of DAG level settings, loops through the array, creates DAG objects and adds them to the global DagBag.

## Settings Array
Depending on who will be controlling the creation of dynamic DAGs in your business, this variable can sit directly in the DAG file itself or it can pulled from Airflow Variables. If done this way, it may be easier for a business user to modify the JSON variable themselves in order to create a new dynamic DAG.

In our example settings array we have four properties on each object.
* Customer Name
    A human readable customer name.
* Customer Id
    A valid string Id. This will be used for the creation of a unique DAG name and connection names.
* Email
    The email array is an example of an override setting we are passing into DAG creation. In this example it is an array of emails that is used to override the ```default_args``` setting object. In order to override the settings of ```default_args``` the property names must be identical in order for ovverride to work.

    The actual overriding of settings takes place in the following code but anything similar will work.
    ```replaced_args = {k: default_args[k] if customer.get(k, None) is None else customer[k] for k in default_args}```
* Enabled
    **TODO: Show programmatic pausing of a DAG using this property.**

#Gotchas

Things to consider:
    - Creating new DAGs with JSON Variable
        - Consider facebook connection it relies on
    - Disabling a DAG via flag on JSON Variable
        - Creating houston wrapper to get and set variables
    - Can you delete a DAG that is created via this method?
    - Startdate and Schedule Interval need to be considered
    - Do we recommend pausing a dyDAG that is not in use?
        - Pausing/Unpause DAG from DAG via CLI

    - Gotchas
        - Changing the name of the variable used to name the dag will result in a new DAG being created
        - Changing the start date