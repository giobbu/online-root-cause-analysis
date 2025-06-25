import time
import random
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
from matplotlib.colors import ListedColormap

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
    custom_cmap = ListedColormap(['blue', 'gray', 'red'])
    fig, axes = plt.subplots(nrows=len(drift_data_stream), ncols=1, figsize=(15, 6), sharex=True)
    for idx, ax in enumerate(axes):
        sns.heatmap(drift_data_stream[idx].reshape(1, -1), 
                    cmap=custom_cmap, 
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


def sudden_mean_drift(drift_info, params):
    """ Simulates a sudden mean drift in sensor data.
    Args:
        drift_info (DriftParams): Information about the drift event.
        params (SensorParams): Parameters of the sensor.
    """
    data_value = np.random.normal(drift_info.mu_drift, params.sigma) + params.eps * np.random.normal(0, 1)
    drift = 1
    return data_value, drift

def sudden_sigma_drift(drift_info, params):
    """ Simulates a sudden mean drift in sensor data.
    Args:
        drift_info (DriftParams): Information about the drift event.
        params (SensorParams): Parameters of the sensor.
    """
    assert drift_info.sigma_drift>0, "sigma should be positive"
    data_value = np.random.normal(params.mu, drift_info.sigma_drift) + params.eps * np.random.normal(0, 1)
    drift = 2
    return data_value, drift

def gradual_mean_drift(drift_info, params, time_step:int):
    """ Simulates a sudden mean drift in sensor data.
    Args:
        drift_info (DriftParams): Information about the drift event.
        params (SensorParams): Parameters of the sensor.
        time_step (int): Current time step in the simulation.
    """
    mu_gradual_drift = np.linspace(params.mu, drift_info.mu_drift, drift_info.duration_drift +1)
    data_value = np.random.normal(mu_gradual_drift[time_step - drift_info.t0_drift], params.sigma) + params.eps * np.random.normal(0, 1)
    drift=3
    return data_value, drift

def gradual_sigma_drift(drift_info, params, time_step:int):
    """ Simulates a sudden mean drift in sensor data.
    Args:
        drift_info (DriftParams): Information about the drift event.
        params (SensorParams): Parameters of the sensor.
        time_step (int): Current time step in the simulation.
    """
    sigma_gradual_drift = np.linspace(params.sigma, drift_info.sigma_drift, drift_info.duration_drift +1)
    data_value = np.random.normal(params.mu, sigma_gradual_drift[time_step - drift_info.t0_drift]) + params.eps * np.random.normal(0, 1)
    drift=4
    return data_value, drift

def send_sensor_data(producer, topic: str, params: 'SensorParams', time_step: int):
    """Send sensor data to Kafka topic with specified parameters.
    Args:
        producer (KafkaProducer): Kafka producer instance.
        topic (str): Kafka topic to send data to.
        params (SensorParams): Parameters for the sensor data generation.
        time_step (int): Time step
    Returns:
        tuple: Generated data value and the time taken to send the data.
    """
    noise = params.eps * np.random.normal(0, 1)
    drift = 0
    data_value = np.random.normal(params.mu, params.sigma) + noise

    if params.drifts:
        for _, drift_event in enumerate(params.drifts):
            drift_info = drift_event['drift-event']
            t0 = drift_info.t0_drift
            delta = drift_info.duration_drift
            if t0 <= time_step <= t0 + delta:
                if drift_info.drift_type == "mean-sudden":
                    data_value,\
                        drift = sudden_mean_drift(drift_info=drift_info, 
                                                    params=params)
                elif drift_info.drift_type == "sigma-sudden":
                    data_value,\
                        drift = sudden_sigma_drift(drift_info=drift_info, 
                                                     params=params)
                elif drift_info.drift_type == "mean-gradual":
                    data_value, \
                        drift = gradual_mean_drift(drift_info=drift_info, 
                                                   params=params, 
                                                   time_step=time_step)
                elif drift_info.drift_type == "sigma-gradual":
                    data_value, \
                        drift = gradual_sigma_drift(drift_info=drift_info, 
                                                   params=params, 
                                                   time_step=time_step)
                break

    missing = np.random.random()
    if missing < params.eps_nan:
        data_value = None

    delay_value = np.random.uniform(0, params.max_delay)
    data_sent={'sensor': params.sensor_name,
               'info': {'timestamp_sent': (datetime.datetime.now()-datetime.timedelta(seconds=delay_value)).strftime('%Y-%m-%d %H:%M:%S'),
                        'timestamp_received': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'obs': data_value,
                        'drift': drift}}
    try:
        future= producer.send(topic, value=data_sent)
        record = future.get(timeout=10)
        #print(f"Message delivered: device-{params.sensor_name}:topic-{record.topic}:partition-{record.partition}")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")
            
    return data_value, drift

def generator_streaming_data(producer, params_sens1, params_sens2, params_sens3, SEED=42):
    np.random.seed(SEED)
    i = 0  # time step
    while True:
        # Sensor-1
        data_1, drift_1 = send_sensor_data(
            producer=producer, topic="sensors", params=params_sens1, time_step=i
        )
        # Sensor-2
        data_2, drift_2 = send_sensor_data(
            producer=producer, topic="sensors", params=params_sens2, time_step=i
        )
        # Sensor-3
        data_3, drift_3 = send_sensor_data(
            producer=producer, topic="sensors", params=params_sens3, time_step=i
        )
        # Yield a structured data point
        yield {
            "time_step": i,
            "sensor_1": {"obs": data_1, "drift": drift_1},
            "sensor_2": {"obs": data_2, "drift": drift_2},
            "sensor_3": {"obs": data_3, "drift": drift_3},
        }
        i += 1

if __name__ == "__main__":
    from kafka import KafkaProducer
    from schemas.schema_sensor import SensorParams

    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    params_sens1 = SensorParams(sensor_name="Sensor-1", mu=0, sigma=1, eps=0.1, delay_mode="fix", delay_value=0.5)
    params_sens2 = SensorParams(sensor_name="Sensor-2", mu=5, sigma=1, eps=0.1, delay_mode="random", delay_value=0.2, max_delay=1)
    params_sens3 = SensorParams(sensor_name="Sensor-3", mu=-5, sigma=1, eps=0.1, delay_mode="fix", delay_value=0.3)
    stream_data = generator_streaming_data(producer, params_sens1, params_sens2, params_sens3)
    for data in stream_data:
        print(data)