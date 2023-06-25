## Running the docker

To get started run ``` docker-compose up ``` in root directory.
It will create the PostgresSQL database and start generating the data.
It will create an empty MySQL database.
It will launch the analytics.py script. 

Your task will be to write the ETL script inside the analytics/analytics.py file.

**Execution Instructions:**
-
- `docker-compose build` to build docker images
- `docker-compose up -d` to run docker containers
- from `root` directory of the code, run command `pytest tests/test.py` to execute unit tests

**Note:**
-

- I have added some basic unit tests of the code using `pytest` in `tests` folder. More advanced test cases can be added in real world project
- `Flake8` is used for code linting and proper docstrings and comments are added for code readability
- Right now, ETL job is processing complete data every time it runs but we can add checkpointing mechanisms to support the incremental loading of data. By storing the last processed checkpoint or timestamp, the job can resume from where it left off in case of failures or interruptions. This way we can minimize redundant processing.
- For this scale and medium-sized datasets, pandas work well but for very large datasets we can use Apache Spark for data processing as it distributes data across a cluster, enabling parallel processing and scalability.
- We can also Integrate jobs with a job scheduler like AWS ECS in real-world projects to automate the execution of data processing tasks.
- we can also add configuration files to projects to manage various settings, such as database connections or logging levels making them more flexible and adaptable to different environments.