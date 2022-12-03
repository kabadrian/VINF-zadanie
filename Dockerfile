#Deriving the latest base image
FROM coady/pylucene:9.1

# Any working directory can be chosen as per choice like '/' or '/home' etc
WORKDIR /usr/app/src

COPY . .

CMD ["python3", "-m", "http.server", "80"]