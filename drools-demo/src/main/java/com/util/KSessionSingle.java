package com.util;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

public class KSessionSingle {

	private static KieSession kSession = null;

	private KSessionSingle() {

	}

	public synchronized static KieSession getInstance() {
		if (kSession == null) {
			KieServices ks = KieServices.Factory.get();
			KieContainer kContainer = ks.getKieClasspathContainer();
			kSession = kContainer.newKieSession("session-risk");
		}
		return kSession;
	}
}
