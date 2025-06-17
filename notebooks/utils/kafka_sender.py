import time
import random
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime

def plot_sens_obs(obs_data_stream:np.array, ylim_low:int=-20, ylim_up:int=20, color='blue'):
    """
    Plot sensor data streams
    """
    fig, axes = plt.subplots(nrows=len(obs_data_stream), ncols=1, figsize=(15, 6), sharex=True)
    for idx, ax in enumerate(axes):
        ax.plot(np.arange(len(obs_data_stream[idx])), obs_data_stream[idx], color=color)
        ax.scatter(np.arange(len(obs_data_stream[idx])), obs_data_stream[idx], color=color)
        ax.set_ylabel(f"Sensor-{idx + 1}")
        ax.set_xlabel("Time Step")
        ax.set_ylim(ylim_low, ylim_up)
    plt.tight_layout()
    plt.show()
    
def plot_drift_stream(drift_data_stream:np.array):
    """
    Plot drifts in data streaming
    """
    fig, axes = plt.subplots(nrows=len(drift_data_stream), ncols=1, figsize=(15, 6), sharex=True)
    for idx, ax in enumerate(axes):
        sns.heatmap(drift_data_stream[idx].reshape(1, -1), 
                    cmap="coolwarm", 
                    cbar=False, 
                    linewidths=1, 
                    linecolor='white', 
                    annot=True, 
                    yticklabels=False, 
                    ax=ax)
        ax.set_ylabel(f"Sensor-{idx + 1}")
        ax.set_xlabel("Time Step")
    plt.tight_layout()
    plt.show()


def send_sensor_data(producer, topic: str, params, time_step: int) -> tuple:
    """Send sensor data to Kafka topic with specified parameters.
    Args:
        producer (KafkaProducer): Kafka producer instance.
        topic (str): Kafka topic to send data to.
        params (SensorParams): Parameters for the sensor data generation.
        time_step (int): Time step
    Returns:
        tuple: Generated data value and the time taken to send the data.
    """
    start_time=time.time()
    noise= params.eps * np.random.normal(0, 1)
    drift = False
    data_value = np.random.normal(params.mu, params.sigma) + noise

    if params.drifts:
        for _, drift_event in enumerate(params.drifts):
            t0 = drift_event['drift-event'].t0_drift
            delta = drift_event['drift-event'].delta_drift
            if t0 <= time_step <= t0 + delta:
                mu_drift = drift_event['drift-event'].mu_drift
                data_value = np.random.normal(mu_drift, params.sigma) + noise
                drift = True
                break

    missing = np.random.random()
    if missing < params.eps_nan:
        data_value = None

    delay_value = np.random.uniform(0, params.max_delay)
    data_sent={'sensor': params.sensor_name,
               'info': {'timestamp_sent': (datetime.datetime.now()- datetime.timedelta(seconds=delay_value)).strftime('%Y-%m-%d %H:%M:%S'),
                        'timestamp_received': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'obs': data_value,
                        'drift': drift}}
    try:
        future= producer.send(topic, value=data_sent)
        record = future.get(timeout=10)
        #print(f"Message delivered: device-{params.sensor_name}:topic-{record.topic}:partition-{record.partition}")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")
        
    end_time=time.time()-start_time
    
    return data_value, drift, end_time

def generator_streaming_data(producer, params_sens1, params_sens2, params_sens3, SEED=42):
    np.random.seed(SEED)
    i = 0  # time step

    while True:

        
        # Sensor-1
        data_1, drift_1, delay_1 = send_sensor_data(
            producer=producer, topic="sensors", params=params_sens1, time_step=i
        )
        # Sensor-2
        data_2, drift_2, delay_2 = send_sensor_data(
            producer=producer, topic="sensors", params=params_sens2, time_step=i
        )
        # Sensor-3
        data_3, drift_3, delay_3 = send_sensor_data(
            producer=producer, topic="sensors", params=params_sens3, time_step=i
        )
        
        # Yield a structured data point
        yield {
            "time_step": i,
            "sensor_1": {"obs": data_1, "drift": drift_1, "delay": delay_1},
            "sensor_2": {"obs": data_2, "drift": drift_2, "delay": delay_2},
            "sensor_3": {"obs": data_3, "drift": drift_3, "delay": delay_3},
            "total_delay": delay_1 + delay_2 + delay_3
        }

        i += 1

if __name__ == "__main__":
    
    # Example usage
    from kafka import KafkaProducer
    from schemas.schema_sensor import SensorParams

    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    params_sens1 = SensorParams(sensor_name="Sensor-1", mu=0, sigma=1, eps=0.1, delay_mode="fix", delay_value=0.5)
    params_sens2 = SensorParams(sensor_name="Sensor-2", mu=5, sigma=1, eps=0.1, delay_mode="random", delay_value=0.2, max_delay=1)
    params_sens3 = SensorParams(sensor_name="Sensor-3", mu=-5, sigma=1, eps=0.1, delay_mode="fix", delay_value=0.3)

    stream_data = generator_streaming_data(producer, params_sens1, params_sens2, params_sens3)
    for data in stream_data:
        print(data)