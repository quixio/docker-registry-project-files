import os
from quixstreams import Application, State
import json

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application(consumer_group="default-group", auto_offset_reset="earliest", use_changelog_topics=False)

# get the input and output topics from environment variables or use a default
# change these to suit your kafka topics
input_topic = app.topic(os.getenv("input", "input-topic"))
output_topic = app.topic(os.getenv("output", "output-topic"))

sdf = app.dataframe(input_topic)

# Filter items out without 'my_value' value.
sdf = sdf[sdf["my_value"].notnull()] 

# Calculate hopping window of 'my_value'. 
# 1 second window with 200ms steps.
sdf = sdf.apply(lambda row: row["my_value"]) \
        .hopping_window(1000, 200).mean().final() 

# function accepts data as dict and optionally, state.
def func(data: dict, state: State):
    # read more about stateful processing here:
    # https://quix.io/docs/quix-streams/advanced/stateful-processing.html
    pass

# Apply any transformation by handling the data in a lambda or function.
# Optionally enable state to perform stateful operations.
sdf = sdf.apply(func, stateful=True)

# Print JSON messages in console.
sdf = sdf.update(lambda row: print(json.dumps(row, indent=4)))

# Send the message to the output topic
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)