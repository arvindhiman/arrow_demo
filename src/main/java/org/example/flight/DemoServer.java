package org.example.flight;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;

public class DemoServer {

    public static void main(String[] args) {

        Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
        try (BufferAllocator allocator = new RootAllocator()) {
            // Server
            try (final CustomFlightCookbook producer = new CustomFlightCookbook(allocator);
                 final FlightServer flightServer = FlightServer.builder(allocator, location, producer).build()) {
                try {
                    flightServer.start();
                    System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                while(true) {}


            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
