from fastapi import FastAPI, Request, HTTPException
from kafka import KafkaProducer
import json
from config.kafka_config import kafka_config
from config.settings import app_settings

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers=kafka_config.bootstrap_servers.split(","),
    security_protocol=kafka_config.security_protocol,
    sasl_mechanism=kafka_config.sasl_mechanism,
    sasl_plain_username=kafka_config.sasl_plain_username,
    sasl_plain_password=kafka_config.sasl_plain_password.get_secret_value(),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_topic(event_type: str) -> str:
    event_mapping = {
        "course_viewed": "moodle_course_views",
        "user_loggedin": "moodle_user_logins",
        "user_loggedout": "moodle_user_logouts",
    }
    short_name = event_type.split("\\")[-1]
    return event_mapping.get(short_name, kafka_config.topic_target)

@app.post("/webhook")
async def handle_webhook(request: Request):
    auth_token = request.headers.get("Authorization", "").split(" ")[-1]
    if auth_token != app_settings.webhook_token:
        raise HTTPException(status_code=403, detail="Invalid token")

    payload = await request.json()
    topic = get_topic(payload.get("eventtype", ""))
    producer.send(topic, value=payload)

    return {"status": "success", "topic": topic}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)