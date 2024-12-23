# Real-Time Object Detection with Kafka and YOLOv3

This project demonstrates real-time object detection on video streams using Apache Kafka, PySpark, OpenCV, and YOLOv3. It processes video frames streamed via Kafka and uses YOLOv3 to detect and classify objects.

## Technologies Used
- **Apache Kafka**: For video stream ingestion.
- **PySpark**: For distributed processing of video frames.
- **OpenCV**: For video frame handling and preprocessing.
- **YOLOv3**: For object detection.

## Setup Instructions

### Prerequisites
1. **Install Kafka locally**:
   - Follow the [Kafka quick start guide](https://kafka.apache.org/quickstart) to set up Kafka on your local machine.
   - Ensure Kafka and Zookeeper are running.

2. **Download YOLOv3 weights**:
   - Download the YOLOv3 pre-trained weights from [YOLO website](https://pjreddie.com/darknet/yolo/).
   - Place the `yolov3.weights` file in the `weights/` directory.

3. **Install dependencies**:
   - Clone the repository:
     ```bash
     git clone https://github.com/acbikash13/kafkaObjectDetection
     cd real-time-object-detection
     ```
   - Install required Python libraries:
     ```bash
     pip install -r requirements.txt
     ```

4. **Configure Kafka settings**:
   - Update the `config.py` file with your local Kafka broker settings (e.g., `localhost:9092`).

### Running the Project

1. **Start Kafka Consumer**:
   - Run the Kafka consumer script to start consuming video frames:
     ```bash
     python consumer.py
     ```

2. **Start Video Stream Producer** (if not already producing):
   - Run the video stream producer to send video frames to Kafka:
     ```bash
     python producer.py
     ```


### Configuration
- **Kafka**: Ensure Kafka is properly configured and running locally before starting the project.
- **YOLO Weights**: Make sure the `yolov3.weights` file is placed in the correct directory (`weights/`).
  
## License
MIT License. See [LICENSE](LICENSE) for more details.

## Acknowledgements
- YOLOv3 for real-time object detection.
- Apache Kafka and PySpark for handling real-time streaming and distributed processing.
