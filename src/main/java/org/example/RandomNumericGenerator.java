package org.example;

import java.util.Random;

public class RandomNumericGenerator {

    public static float getRandomFloat() {
        Random random = new Random();
        return random.nextFloat();
    }
}
