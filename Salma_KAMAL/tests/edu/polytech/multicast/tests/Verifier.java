package edu.polytech.multicast.tests;

import edu.polytech.multicast.Actor;

public class Verifier {
    
    // Cette méthode sera appelée directement par les acteurs, pas par message QueueBroker
    public synchronized void verifyDelivery(Actor actor, Object message) {
        // TODO: Vérifier que 'message' respecte l'ordre total pour tous les acteurs
    }

    public synchronized void notifyDeath(Actor actor) {
        // TODO: Enregistrer la mort pour les logs de test
    }
    
    public synchronized void notifyResurrection(Actor actor) {
        // TODO: Enregistrer le retour
    }
}
