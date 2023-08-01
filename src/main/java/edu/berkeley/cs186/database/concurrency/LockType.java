package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * according the LockType sequence
     * give the compatible matrix
     * each index is the original enum type index
     * row seq: S,X,IS,IX,SIX,NL
     * col seq: S,X,IS,IX,SIX,NL
     */
    private static boolean[][] compatibleMatrix = {
            {true, false, true, false, false, true},
            {false, false, false, false, false, true},
            {true, false, true, true, true, true},
            {false, false, true, true, false, true},
            {false, false, true, false, false, true},
            {true, true, true, true, true, true}
    };

    /**
     * according the LockType sequence
     * give the parent matrix
     * each index is the original enum type index
     * row seq: S,X,IS,IX,SIX,NL
     * col seq: S,X,IS,IX,SIX,NL
     */
    private static boolean[][] parentMatrix = {
            {false, false, false, false, false, true},
            {false, false, false, false, false, true},
            {true, false, true, false, false, true},
            {true, true, true, true, true, true},
            {false, true, false, true, false, true},
            {false, false, false, false, false, true}
    };

    /**
     * according the LockType sequence
     * give the substitutability matrix
     * each index is the original enum type index
     * row seq: S,X,IS,IX,SIX,NL
     * col seq: S,X,IS,IX,SIX,NL
     */
    private static boolean[][] substitutabilityMatrix = {
            {true, false, true, false, false, true},
            {true, true, true, true, true, true},
            {false, false, true, false, false, true},
            {false, false, true, true, false, true},
            {true, false, true, true, true, true},
            {false, false, false, false, false, true}
    };

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        int x = a.ordinal();
        int y = b.ordinal();
        return compatibleMatrix[x][y];
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        int x = parentLockType.ordinal();
        int y = childLockType.ordinal();
        return parentMatrix[x][y];
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     * means using substitute lockType can cover all situations of required lockType
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        int x = substitute.ordinal();
        int y = required.ordinal();
        return substitutabilityMatrix[x][y];
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

