package io.pivotal.kieservicebroker;

public class BusinessException extends Throwable {

	public BusinessException(String string, Throwable original) {
		// TODO Auto-generated constructor stub

		System.out.println("##### business exception string=" + string);
		System.out.println("##### business exception original=" + original);
	}

}
