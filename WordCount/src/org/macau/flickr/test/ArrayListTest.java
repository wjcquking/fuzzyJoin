package org.macau.flickr.test;

import java.util.ArrayList;

public class ArrayListTest {

	public static void main(String[] args){
		ArrayList<Integer> list = new ArrayList<Integer>();
		
		for(int i = 0; i < 1;i++){
			list.add(i);
		}
		System.out.println(list);
		
		System.out.println(list.toString().substring(1,list.toString().length()-1));

	}
}
