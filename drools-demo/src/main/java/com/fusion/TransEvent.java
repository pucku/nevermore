package com.fusion;

import java.io.Serializable;
import java.util.Date;

public class TransEvent implements Serializable {
	private String address;
	private String card;
	private String behavior;
	private Date eventtime;
	private long sendtime;

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getCard() {
		return card;
	}

	public void setCard(String card) {
		this.card = card;
	}

	public String getBehavior() {
		return behavior;
	}

	public void setBehavior(String behavior) {
		this.behavior = behavior;
	}

	public Date getEventtime() {
		return eventtime;
	}

	public void setEventtime(Date eventtime) {
		this.eventtime = eventtime;
	}

	public long getSendtime() {
		return sendtime;
	}

	public void setSendtime(long sendtime) {
		this.sendtime = sendtime;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return address + ":" + card + ":" + behavior + ":" + eventtime.toString();
	}

}
