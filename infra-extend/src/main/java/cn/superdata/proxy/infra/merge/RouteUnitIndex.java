package cn.superdata.proxy.infra.merge;

import lombok.Data;

@Data
public class RouteUnitIndex {
	private final int colIndexInRouteUnit;
	private final int routeUnitIndex;

	public RouteUnitIndex(int colIndexInRouteUnit, int routeUnitIndex) {
		this.colIndexInRouteUnit = colIndexInRouteUnit;
		this.routeUnitIndex = routeUnitIndex;
	}

}
