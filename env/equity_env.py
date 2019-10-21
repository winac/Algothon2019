import pandas as pd
import numpy as np

from gym import Env, spaces

from sklearn.preprocessing import StandardScaler

class EquityEnv(Env):

    metadata = {'render.modes': ['human']}

    def __init__(self, equity_name, date_range, timesteps=3):
        
        self.equity_name = equity_name
        self.date_range = date_range
        self.timesteps = timesteps

        observation_range = np.finfo('float32').min, np.finfo('float32').max
        action_range = -1, 1

        self.full_data, self.end_index = self._read_data(equity_name)

        self.features = self._get_features(self.full_data)
        self.equity_returns = self._get_equity_returns(self.full_data)

        self.observation_space = spaces.Box(*observation_range,
                                            shape=(timesteps, self.full_data.shape[1]+1), dtype=np.float32)
        self.action_space = spaces.Box(*action_range, (1,), dtype=np.float32)

        self.usd_exposure = None
        self.done = False
        self.index = None

        self.transaction_pct = 0.00050
        self.returns = [0]
        self.exposure_history = np.zeros(self.timesteps)

    def _get_observation(self):

        features = self.features[self.index-self.timesteps:self.index]
        exposures = np.expand_dims(self.exposure_history[-self.timesteps:], 1)

        return np.append(features, exposures, axis=1)

    def step(self, exposure):
        
        transaction_costs = abs((self.usd_exposure-exposure))*self.transaction_pct
        self.usd_exposure = exposure

        self.index += 1

        step_return = self.equity_returns[self.index] * self.usd_exposure
        total_return = float(step_return - transaction_costs)

        self.returns.append(total_return)
        self.exposure_history = np.append(self.exposure_history, exposure)

        observation = self._get_observation()
        reward = total_return
        done = True if (self.index == self.end_index - 1) else False

        return observation, reward, done, {}

    def reset(self):

        self.usd_exposure = 0

        self.exposure_history = np.zeros(self.timesteps)
        self.returns = [0]

        self.done = False
        self.index = self.timesteps

        observation = self._get_observation()

        return observation

    def render(self, mode='human', close=False):
        return self.returns, self.exposure_history

    def _get_equity_returns(self, full_data):

        equity_price = full_data['{}.Close'.format(self.equity_name)]
        equity_returns = equity_price.pct_change().fillna(0)

        return np.array(equity_returns)

    def _read_data(self, equity):

        full_data = pd.read_csv('data/{}.csv'.format(equity), index_col='Date').dropna()
        full_data = full_data[self.date_range[0]:self.date_range[1]]
        full_data_len = len(full_data)

        return full_data, full_data_len

    @staticmethod
    def _get_features(full_data):

        scaler = StandardScaler()

        features = full_data.astype(np.float32)
        features = features.diff().fillna(0)
        features = scaler.fit_transform(features)

        return features
