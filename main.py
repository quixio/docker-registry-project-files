from quixstreams import Application, State
import json
import uuid

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

# Connect to the Quix public broker to consume data
app = Application(
    broker_address="publickafka.quix.io:9092",  # Kafka broker address
    consumer_group=str(uuid.uuid4()),  # Kafka consumer group
    auto_offset_reset="latest",  # Read topic from the end
    producer_extra_config={"enable.idempotence": False},
    use_changelog_topics=False
)

# set the topic name. In this case the topc that's available on the public Kafka.
input_topic = app.topic("demo-onboarding-prod-chat", value_deserializer="json")
# (note: the public Kafka is readonly, enable the output topic for your own Kafka)
# output_topic = app.topic("my_output_topic")

sdf = app.dataframe(input_topic)

sdf["tokens_count"] = sdf["message"].apply(lambda message: len(message.split(" ")))
sdf = sdf[["role", "tokens_count"]]

# Calculate hopping window of "tokens_count". 
# 1 second window with 200ms steps.
sdf = sdf.apply(lambda row: row["tokens_count"]) \
        .hopping_window(1000, 200).mean().final() 

# function accepts data as dict and optionally, state.
def func(data: dict, state: State):
    
    # sum the total of value and keep it in state
    # read more about stateful processing here:
    # https://quix.io/docs/quix-streams/advanced/stateful-processing.html

    # get sum from state, default to 0 if not found
    value_sum = state.get("sum", 0)
    # add value to value_sum to track the total
    value_sum += data["value"]
    # store updated sum back to state
    state.set("sum", value_sum)

    # return data in your desired schema
    return {
        "avg_tokens": data["value"],
        "sum": value_sum
    }

# Apply any transformation by handling the data in a lambda or function.
# Optionally enable state to perform stateful operations.
sdf = sdf.apply(func, stateful=True)

# Print JSON messages in console.
sdf = sdf.update(lambda row: print(json.dumps(row, indent=4)))

# Send the message to the output topic
# (note: the public Kafka is readonly, enable for your own Kafka)
# sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)