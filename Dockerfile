FROM python:3.12.2-slim
# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . .
# Switch to the non-privileged user to run the application.
RUN pip install --no-cache-dir -r requirements.txt
# Copy the source code into the container.

# # Expose the port that the application listens on.
EXPOSE 3000

# Run the application.
ENTRYPOINT ["dasgster", "dev", "-f", "git_csv_turso_init.py"]
