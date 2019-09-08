make:
	python3 app.py

prod:
	sudo python3 app.py p &

pid:
	sudo netstat -peanut | grep python
