package org.example;

import java.util.Random;

public class RandomCusipGenerator {
    static String PREFIX = "ARD";

    public static String getRandom() {
        String cusip8 = PREFIX + randomAlphanumeric(5).toUpperCase();
        return cusip8 + getCheckDigit(cusip8);
    }

    public static boolean isValid(String cusip) {
        return cusip.length() == 9 && getCheckDigit(cusip.substring(0, 8)) == cusip.charAt(8);
    }

    private static String randomAlphanumeric(int targetStringLength) {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();
        return random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private static char getCheckDigit(String cusip8) {
        int sum = 0;
        char[] digits = cusip8.toCharArray();
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ*@#";
        for (int i = 0; i < digits.length; i++) {
            int val;
            if (!Character.isDigit(digits[i])) {
                val = alphabet.indexOf(digits[i]) + 10;
            } else {
                val = Character.getNumericValue(digits[i]);
            }
            if ((i % 2) != 0) {
                val *= 2;
            }
            val = (val % 10) + (val / 10);
            sum += val;
        }
        return Integer.toString((10 - (sum % 10)) % 10).charAt(0);
    }

}
