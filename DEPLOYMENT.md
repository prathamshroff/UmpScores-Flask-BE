# Deployment Guide

## Introduction

This guide provides step-by-step instructions on how to deploy the umpscores backend API on an AWS EC2 instance using Flask. The deployment process requires manual steps and is intended for full-stack developers working for UmpScores. 

## Server Setup

1. Log in to the AWS Management Console and navigate to EC2
2. Launch an EC2 instance using the preferred AMI and instance type
3. Configure the instance security group to allow inbound traffic on ports 80 and 22
4. Download the .pem file for your EC2 instance
5. Change the permissions of the .pem file by running the following command: `chmod 600 /path/to/your/pem/file.pem`
6. SSH into the EC2 instance by running the following command: `ssh -i /path/to/your/pem/file.pem ubuntu@ec2-xx-xx-xxx-xxx.compute-1.amazonaws.com`
7. Setup & Install python3, pip3, tmux, docker (optional) and other dependencies as prompted

Note: If you're an UmpScores Developer with access to our AWS, just select the "Refrating-BE" Instance in EC2 and ask your manager to share the .pem file.

## Deployment Steps

1. Clone the backend repository using `git clone <repository URL>`
2. Change into the repository directory using `cd <your-repository>` 
3. Create a virtual environment and activate it using `python3 -m venv venv` and then `source venv/bin/activate`
4. Create a `.config.json` file in the root directory of the application and populate the file with the necessary configuration settings, including the database connection string, privileged API key, and other settings as required.
5. Install the required dependencies using `pip3 install -r requirements.txt`
6. Start a tmux session using `tmux new -s <session-name>`
7. Start the Flask server using `make prod`
8. The application should now be accessible via a web browser or API client at http://<EC2 instance IP address>:80
9. Detach from the tmux session by pressing `Ctrl-b` and then `d`
10. Close the SSH connection using `exit`
  
Note: If you're an UmpScores Developer, you wouldn't need to do any of this unless you have pushed changes to 'master' on GitHub & want to update the instance with essentially, a new codebase.
  
## Additional Notes
The application is running on port 80 because it is the default port for HTTP traffic. Port 22 is used for SSH connections.

Using tmux allows us to keep the Flask server running even when we are not connected to the server. We can also reattach to the tmux session to check the server logs or make changes to the code without having to restart the server.

Don't forget to update the .env file with the correct credentials for your production environment.
