package com.lamda.util;

import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;

public abstract class AwsCredentialsConfig {

	protected static AWSCredentials credentials = null;
	static {
		try {
			credentials = new PropertiesCredentials(
					AwsCredentialsConfig.class.getResourceAsStream("/awscredentials.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error getting credentials"+e.getMessage());
			e.printStackTrace();
		}
	}
}
