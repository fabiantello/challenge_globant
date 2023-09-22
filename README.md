# challenge_globant

For using this project in local enviroment, it's required to have installer docker desktop on your machine. ----> https://www.docker.com/

After the installation of docker, clone this GitHub project in your machine and open a command prompt. In the console, run the next line command, replacing LOCAL_PATH for the path in your machine when you cloned the project.

docker compose -f LOCAL_PATH\docker-compose.yml up

Once the container are running, for entering the rest_api, open your browser and enter this url: http://localhost:8000/docs 
There, in the Authorize button at the up-right side, enter the credentials, which in this case is "globant", without quotes, both for user and password.

After this steps, you're now able to use the three endpoints for load new data from csv files into the postgresql database.
