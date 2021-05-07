# Country Lookup Pipeline

## How to run the pipeline

1. The twitter client variables must be set in a .env file. Visit [this blog post](https://gfajardo.medium.com/part-i-creating-a-twitter-stream-6dc6d1ce06c) to learn the setup.
2. Upload the cities.csv file to a GCP Storage bucket. Further GCP configurations are left for the reader to setup.
3. Run the twitter/publisher.py file in an active virtual environment. Also, the virtual environment needs to have the dependencies installed--found in the requirements.txt file.
4. Run the Spring Boot application.
5. Trigger the pipeline by going to http://localhost:8080/pipeline
