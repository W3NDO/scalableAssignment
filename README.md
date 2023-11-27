# A comparison of ArangoDB and MongoDB

## Resources Used
1. https://hub.docker.com/r/arangodb/arangodb
2. https://docs.arangodb.com/3.11/operations/administration/configuration/
3. https://stackoverflow.com/questions/46147792/arangodb-real-execution-query-time : About logging Query time in Arango
4. Dataset https://www.kaggle.com/datasets/rtatman/between-our-worlds-an-anime-ontology ( https://betweenourworlds.org/ )


## Setting Up Arango Docker

To set up the arangodb docker image

1. pull the image from docker with this command
    'docker pull arangodb/arangodb'

2. Start a docker instace with this command
    'docker run -e ARANGO_ROOT_PASSWORD=pswd1 -p 8529:8529 -d --name scalableArango arangodb/arangodb'

    ```
        // note that the password can be anything, but do not forget it.
        // the variable you pass to -name will be the name of your container
    ```

3. start the arango interactive shell with this command
    'docker exec -it scalableArango arangosh'

    ```
        // 'scalableArango2' is the name of our container
        // arangosh is the arango shell command.
    ```