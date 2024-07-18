# Process data in Kafka with Python using Quix Streams

The official Docker image for processing data in Kafka with Python using Quix Streams

This application demonstrates how to process data in Kafka with Python using [Quix Streams](https://quix.io/docs/quix-streams/introduction.html). It uses data in a publicly available read-only Kafka cluster and illustrates how to process the data with a stateful hopping window operator.

This application assumes the data will contain a field called `my_value`.

You can adapt this code to suit your needs.

## How to run

 - Create a virtual environment: `python -m venv venv` (`python3` on Mac)
 - Install the requirements: `pip install -r requirements.txt`
 - Run `main.py`: `python main.py` (`python3` on Mac)

To get started with running projects like this locally [read the docs](https://quix.io/docs/quix-cli/overview.html).

## Resources

[Quix Streams Quick Start](https://quix.io/docs/get-started/quix-start.html)
[Quix CLI Quick Start](https://quix.io/docs/quix-cli/cli-quickstart.html)
[Quix Cloud Quick Start](https://quix.io/docs/quix-cloud/quickstart.html)

## Template gallery

Visit Quix [YouTube](https://www.youtube.com/@QuixStreams) or Quix Community [Slack](https://quix.io/slack-invite?_gl=1*x8ux8c*_gcl_au*MTQ2NjM5MDQ2OS4xNzE3NTE4ODI5*_ga*MTA3MDczODk0LjE3MTc1MTg4Mjk.*_ga_BFBHQ33YP1*MTcyMTExODAzNS40NC4xLjE3MjExMTgzNjIuMC4wLjA.&_ga=docker_reg_1) to learn more about developing real-time data pipelines with Kafka and Python.

## Contribute

We welcome contributions, see the [contribution guide](https://github.com/quixio/quix-streams/blob/main/CONTRIBUTING.md) and join the community [Slack](https://quix.io/slack-invite?_gl=1*x8ux8c*_gcl_au*MTQ2NjM5MDQ2OS4xNzE3NTE4ODI5*_ga*MTA3MDczODk0LjE3MTc1MTg4Mjk.*_ga_BFBHQ33YP1*MTcyMTExODAzNS40NC4xLjE3MjExMTgzNjIuMC4wLjA.&_ga=docker_reg_1).

## Open source

This project is open source under the Apache 2.0 license and available in [GitHub](https://github.com/quixio/quix-streams).

Star and share the repo to show your appreciation.