FROM python:3.9

# Non-Root User: To Improve Security
RUN adduser --disabled-password appuser
USER appuser

WORKDIR /app

COPY requirements.txt requirements.txt

# Install Dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Extra-Safe: To Mitigate Frequent NotFound Errors
RUN pip3 install --no-cache-dir MLB-statsapi
RUN pip3 install --no-cache-dir simplejson

COPY . .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]
