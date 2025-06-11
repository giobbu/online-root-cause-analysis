import time
import random
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def set_sensor_delay(params):
    """
    Set sensor delay either fix or random mode
    Args:
        params (SensorParams): Parameters for the sensor data generation.
    """
    if params.delay_mode == "fix":
        time.sleep(params.delay_value)
    elif params.delay_mode == "random":
        if params.max_delay is None:
            raise ValueError("max_delay should be specified in delay_mode 'random'")
        time.sleep(random.uniform(params.delay_value, params.max_delay))
    else:
        raise ValueError("delay_mode should be 'fix' or 'random'")

def plot_sens_obs(obs_data_stream:np.array, ylim_low:int=-20, ylim_up:int=20):
    """
    Plot sensor data streams
    """
    fig, axes = plt.subplots(nrows=len(obs_data_stream), ncols=1, figsize=(15, 6), sharex=True)
    for idx, ax in enumerate(axes):
        ax.plot(np.arange(len(obs_data_stream[idx])), obs_data_stream[idx])
        ax.scatter(np.arange(len(obs_data_stream[idx])), obs_data_stream[idx])
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