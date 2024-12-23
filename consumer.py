from kafka import KafkaConsumer
import cv2
import pickle
import numpy as np
import torch
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from ultralytics import YOLO


# Load YOLO model
def load_yolo_model(config_path, weights_path):
    """Load the YOLO model."""
    net = cv2.dnn.readNet(weights_path, config_path)
    output_layers = net.getUnconnectedOutLayersNames()
    return net, output_layers


def detect_objects(frame, net, output_layers, confidence_threshold=0.5):
    """Run YOLO on a frame to detect objects."""
    height, width, channels = frame.shape

    # Prepare the frame for YOLO
    blob = cv2.dnn.blobFromImage(frame, 0.00392, (416, 416), (0, 0, 0), True, crop=False)
    net.setInput(blob)

    # Run forward pass and get detections
    outputs = net.forward(output_layers)
    boxes, confidences, class_ids = [], [], []

    #This function process the frames, calculates the confidence scores and determines the class of the object and takes the frame in which the object is located.
    for output in outputs:
        for detection in output:
            scores = detection[5:]
            class_id = np.argmax(scores)
            confidence = scores[class_id]
            if confidence > confidence_threshold:
                # Object detected
                center_x, center_y, w, h = (detection[0:4] * np.array([width, height, width, height])).astype('int')
                x = int(center_x - w / 2)
                y = int(center_y - h / 2)
                boxes.append([x, y, w, h])
                confidences.append(float(confidence))
                class_ids.append(class_id)

    return boxes, confidences, class_ids


def process_frame(frame, net, output_layers, classes_path):
    """Process the frame using YOLO and return detection results."""
    with open(classes_path, "r") as f:
        classes = [line.strip() for line in f.readlines()]

    boxes, confidences, class_ids = detect_objects(frame, net, output_layers)

    results = []
    for i in range(len(boxes)):
        label = str(classes[class_ids[i]])  # Use updated classes
        confidence = confidences[i]
        results.append((boxes[i], label, confidence))

    return results


def process_rdd(rdd, net, output_layers, classes_path):
    """Process an RDD of frames."""
    if not rdd.isEmpty():
        frames = rdd.collect()
        results = []
        for frame in frames:
            results.append(process_frame(frame, net, output_layers, classes_path))
        display_results(results)


def create_spark_streaming_context():
    """Create a SparkContext and StreamingContext."""
    sc = SparkContext(appName="KafkaObjectDetection")
    ssc = StreamingContext(sc, 1)  # batch = 1 means Process every 1 second
    return sc, ssc


def display_results(results):
    """Display object detection results."""
    for frame_results in results:
        for box, label, confidence in frame_results:
            print(f"Detected: {label} with confidence: {confidence:.2f}")
            # Here you could display the frame or do further processing.


def consume_kafka_messages(bootstrap_servers, topic, batch_size=10):
    """Consume Kafka messages using KafkaConsumer."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="spark-streaming-consumer",
        auto_offset_reset="earliest"
    )

    # Consume messages in batches
    messages = []
    for message in consumer:
        # Deserialize the frame (assuming frames are pickled)
        frame = pickle.loads(message.value)
        messages.append(frame)

        if len(messages) >= batch_size:
            yield messages
            messages = []  # Reset for next batch


def kafka_streaming_rdd(sc, bootstrap_servers, topic):
    """Create a DStream-like structure from Kafka Consumer."""
    rdd_list = []

    # Using an iterator to simulate streaming RDD
    for message_batch in consume_kafka_messages(bootstrap_servers, topic):
        rdd_list.append(sc.parallelize(message_batch))  # Convert the list of frames to an RDD

    return rdd_list


if __name__ == "__main__":
    bootstrap_servers = 'localhost:9092'
    topic = 'test'
    config_path = "yolov3.cfg"  # Path to YOLO config file
    weights_path = "yolov3.weights"  # Path to YOLO weights
    classes_path = "coco.names"  # Path to class names file

    # Load YOLO model
    net, output_layers = load_yolo_model(config_path, weights_path)

    # Create Spark Streaming Context
    sc, ssc = create_spark_streaming_context()

    # streaming source using KafkaConsumer
    kafka_rdd_stream = kafka_streaming_rdd(sc, bootstrap_servers, topic)

    # Process each RDD (simulated stream) in Spark
    for rdd in kafka_rdd_stream:
        process_rdd(rdd, net, output_layers, classes_path)

    # Start the Spark Streaming Context (although we are using simulated batches)
    ssc.start()
    ssc.awaitTermination()
