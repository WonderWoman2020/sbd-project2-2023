package app;


import ui.UIController;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;

public class App
{
    public static void main( String[] args )
    {
        UIController uiController = UIController.builder().build();
        try {
            uiController.inputLoop();
        } catch (IOException | InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e);
        }
    }
}
