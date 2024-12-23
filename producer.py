from kafka import KafkaProducer
import cv2
import pickle


def create_producer(bootstrap_servers):
    "Create and return a Kafka producer."
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: pickle.dumps(v)  # Serialize data to send frames
    )
    return producer



# change thew camera index based on the camera you are using. if a default laptop camera, index = 0
def stream_camera(producer, topic, camera_index=1, frame_skip=10):
    """Capture frames from the camera and send to Kafka topic."""
    cap = cv2.VideoCapture(camera_index)
    if not cap.isOpened():
        print("Error: Camera cannot be opened")
        return

# to make the detection of the object smoother, I am skipping 10 frames. It will provide a smoother experience in seeing the Consumer Detected video feed.
    print("Streaming started...")
    frame_count = 0
    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                print("Error: Cannot read frame")
                break
            # Skip frames for optimization
            frame_count += 1
            if frame_count % frame_skip != 0:
                continue

            # Resize frame to reduce payload size
            frame_resized = cv2.resize(frame, (320, 240))
            producer.send(topic, frame_resized)

            # Display the current camera feed
            cv2.imshow("Camera Feed (Press 'q' to quit)", frame_resized)
            if cv2.waitKey(1) & 0xFF == ord('q'):
                print("Streaming stopped")
                break
    finally:
        cap.release()
        cv2.destroyAllWindows()


if __name__ == "__main__":
    bootstrap_servers = 'localhost:9092'
    topic = 'video-stream'
    producer = create_producer(bootstrap_servers)
    stream_camera(producer, topic)
