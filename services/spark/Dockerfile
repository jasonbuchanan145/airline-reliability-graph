# Use the official OpenJDK 8 base image
FROM eclipse/ubuntu_jdk8

# Set the working directory inside the container
WORKDIR /app

# Copy the source code into the container
COPY . /app

# Change to the Spark service directory
WORKDIR /app/

# Expose the port that the Spring Boot application will run on
EXPOSE 8080

# Run the Spring Boot application
CMD ["mvn", "spring-boot:run"]