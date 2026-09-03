FROM python:3.12-slim

WORKDIR /sim

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY robot.py supervisor.py ./

# The supervisor forks one OS process per robot in robots.json. Starting it as the
# container's command is what satisfies "we should not need to start it by hand in a
# separate terminal".
CMD ["python", "-u", "supervisor.py"]
