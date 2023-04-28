FROM python:3.9

# Non-Root User: To Improve Security
RUN adduser --disabled-password appuser
USER appuser

WORKDIR /app

COPY requirements.txt requirements.txt

# Install Dependencies
RUN pip3 install -r requirements.txt

# Extra-Safe: Frequent Not Found Errors
RUN pip3 install MLB-statsapi
RUN pip3 install simplejson

# Remove Unused Dependencies to Reduce Image Size 
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]
