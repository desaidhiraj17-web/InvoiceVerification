from loguru import logger
import sys

# Remove the default logger
logger.remove()

# Add file logger
logger.add(
    "app.log",                  # log file name
    rotation="45 days",            # rotate when file exceeds 1 MB
    retention="10 days",         # delete logs older than 10 days
    compression="zip",           # compress old logs to save space
    level="DEBUG",               # log level
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    enqueue=True
)

# # Optional: also log to console
# logger.add(sys.stdout, level="INFO")
