package com.anomaly_detection_stream.model;

public class UserClick {
	private String user;
	private String ipAddress;

	public UserClick(String user, String ipAddress) {
		this.user = user;
		this.ipAddress = ipAddress;
	}

	public UserClick() {
	}

	public String getUser() {
		return user;
	}

	public String getIpAddress() {

		return ipAddress;
	}

}
