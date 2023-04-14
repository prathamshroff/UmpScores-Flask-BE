# Run the application in development mode
make:
	python3 app.py

# Run the application in production mode (background process)
prod:
	if pgrep -f app.py >/dev/null; then \
		echo "Application is already running"; \
	else \
		sudo python3 app.py p & \
	fi

# Display a list of processes running the application
pid:
	sudo netstat -peanut | grep python

# Install application dependencies
requirements:
	pip3 install -r requirements.txt

# Remove any compiled or temporary files, as well as the __pycache__ directory
clean:
	rm -rf *.pyc __pycache__
