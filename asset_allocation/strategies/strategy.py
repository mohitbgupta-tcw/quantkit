import quantkit.asset_allocation.return_calc.log_return as log_return
import quantkit.asset_allocation.return_calc.ewma_return as ewma_return
import quantkit.asset_allocation.return_calc.simple_return as simple_return
import quantkit.asset_allocation.risk_calc.log_vol as log_vol
import quantkit.asset_allocation.risk_calc.ewma_vol as ewma_vol
import quantkit.asset_allocation.risk_calc.simple_vol as simple_vol


class Strategy(object):
    def __init__(self, universe, return_engine, risk_engine, frequency, **kwargs):
        risk_return_engine_kwargs = dict(
            frequency=frequency, ddof=1, geo_base=1, adjust=True, half_life=12, span=36
        )

        # return engine
        if return_engine == "log_normal":
            self.return_engine = log_return.LogReturn(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif return_engine == "ewma":
            self.return_engine = ewma_return.LogEWMA(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif return_engine == "ewma_rolling":
            self.return_engine = ewma_return.RollingLogEWMA(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif return_engine == "simple":
            self.return_engine = simple_return.SimpleExp(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        else:
            return_engine = None

        # risk engine
        if risk_engine == "log_normal":
            self.risk_engine = log_vol.LogNormalVol(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif risk_engine == "ewma":
            self.risk_engine = ewma_vol.LogNormalEWMA(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif risk_engine == "ewma_rolling":
            self.risk_engine = ewma_vol.RollingLogNormalEWMA(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        elif risk_engine == "simple":
            self.risk_engine = simple_vol.SimpleVol(
                universe=universe, **risk_return_engine_kwargs, **kwargs
            )
        else:
            risk_engine = None
