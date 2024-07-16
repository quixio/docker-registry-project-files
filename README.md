# Transformation template

This sample was created for inclusion in the Docker Registry and demonstrates how to use [Quix Streams](https://github.com/quixio/quix-streams) to connect to Kafka and use a hopping window to process the data.

This example assumes the data will contain a field called `my_value`.

You can adapt this code to suit your needs.

## How to run

 - Create a virtual environment: `python -m venv venv` (`python3` on Mac)
 - Install the requirements: `pip install -r requirements.txt`
 - Run `main.py`: `python main.py` (`python3` on Mac)

To get started with running projects like this localls [read the docs](https://quix.io/docs/quix-cli/overview.html).

## Environment variables

The code sample uses the following environment variables:

- **input**: This is the input topic for f1 data.
- **output**: This is the output topic for hard braking events.

## Contribute

We welcome contrinutions, please contact us for a t-shirt or recognition on our community [Slack](https://quix.io/slack-invite?_gl=1*x8ux8c*_gcl_au*MTQ2NjM5MDQ2OS4xNzE3NTE4ODI5*_ga*MTA3MDczODk0LjE3MTc1MTg4Mjk.*_ga_BFBHQ33YP1*MTcyMTExODAzNS40NC4xLjE3MjExMTgzNjIuMC4wLjA.&_ga=docker_reg_1).

## Open source

This project is open source under the Apache 2.0 license and available in our [GitHub](https://github.com/quixio/quix-samples) repo.

Please star us and mention us on social to show your appreciation.