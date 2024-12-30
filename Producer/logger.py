import sys

class Logger:
    def __init__(self, destination):
        self.destination = destination

    def log(self, message: str) -> None:
        if self.destination == sys.stdout or self.destination == sys.stderr:
            print(message, file=self.destination)
        else:
            with open(self.destination, "a", encoding="utf-8") as log_file:
                log_file.write(message + "\n")

