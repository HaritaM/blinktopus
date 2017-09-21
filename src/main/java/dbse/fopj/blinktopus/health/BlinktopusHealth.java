package dbse.fopj.blinktopus.health;

import com.codahale.metrics.health.HealthCheck;

public class BlinktopusHealth extends HealthCheck{

	@Override
	protected Result check() throws Exception {
		// TODO Auto-generated method stub
		return Result.healthy();
	}

}
