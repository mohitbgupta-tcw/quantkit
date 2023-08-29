import quantkit.asset_allocation.return_calc.log_return as log_return
import quantkit.asset_allocation.return_calc.ewma_return as ewma_return
import quantkit.asset_allocation.return_calc.simple_return as simple_return


class Strategy(object):
    def __init__(self, universe, return_engine, risk_engine, frequency, **kwargs):
        risk_return_engine_kwargs = dict(
            frequency=frequency,
            ddof=1,
            geo_base=1,
            adjust=True,
            half_life=12,
        )

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
        self.risk_engine = risk_engine
