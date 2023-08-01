package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException,UnsupportedOperationException{
        // TODO(proj4_part2): implement
        checkReadOnly();
        final LockContext parentContext = parentContext();
        if(parentContext != null && !LockType.canBeParentLock(parentContext.getEffectiveLockType(transaction),lockType)){
            throw new InvalidLockException(String.format("%s is invalid for parent %s", lockType, parentContext));
        }
        lockman.acquire(transaction,getResourceName(),lockType);
        if(parentContext!=null){
            parentContext.updateChildLockNum(transaction,1);
        }
    }

    /**
     * update numChildLocks
     * if children has changed the parent numChildLocks map should be changed too
     * @param transactionContext
     * @param delta
     */
    private void updateChildLockNum(TransactionContext transactionContext,int delta){
        final long transactionNum = transactionContext.getTransNum();
        numChildLocks.compute(transactionNum,(k,v)->{
           if(v == null){
               v = 0;
           }
           v += delta;
           return v;
        });
        if(parent!=null){
            parent.updateChildLockNum(transactionContext,delta);
        }
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        checkReadOnly();
        if(getNumChildren(transaction)!=0){
            throw new InvalidLockException("the release request is invalid");
        }
        lockman.release(transaction,getResourceName());
        final LockContext parent = parentContext();
        if(parent!=null){
            parent.updateChildLockNum(transaction,-1);
        }
        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        checkReadOnly();
        if(!Objects.equals(LockType.SIX,newLockType)){
            lockman.promote(transaction,getResourceName(),newLockType);
            return;
        }
        // deal with SIX lock
        // see parent have SIX lock?
        LockContext parent = parentContext();
        if(hasSIXAncestor(transaction)){
            throw new InvalidLockException(String.format("%s got a SIX ancestor", this));
        }

        // release children lock with S/IS
        final List<ResourceName> releaseNames = sisDescendants(transaction);
        // updateChildLockNum(transaction,releaseNames,-1);

        //substitute self using method: release first and then get the new lock
        releaseNames.add(getResourceName());
        updateChildLockNum(transaction,releaseNames,-1);
        lockman.acquireAndRelease(transaction,getResourceName(),newLockType,releaseNames);
        return;
    }

    /**
     * batch update the numChildLocks
     * @param transactionContext
     * @param releaseNames
     * @param delta
     */
    private void updateChildLockNum(TransactionContext transactionContext,List<ResourceName> releaseNames,int delta){
        releaseNames.stream()
                .map(resourceName -> {return fromResourceName(lockman,resourceName).parentContext();})
                .filter(Objects::nonNull)
                .forEach(parentContext ->{
                    parentContext.updateChildLockNum(transactionContext,delta);
                });
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * in this method we mainly to collect all lockType of children and combine children's type and current type
     * to upgrade
     * most important thing is, in most case we can just enhance cur type into X
     * except when cur type is IS/S and all chlidren is belong to IS/S
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        checkReadOnly();
        LockType prevLockType = getExplicitLockType(transaction);
        if(Objects.equals(LockType.NL,prevLockType)){
            throw new UnsupportedOperationException(String.format("transaction %s has no lock at this level",transaction));
        }
        Predicate<LockType> toXPredicate = lockType -> {
          return Objects.equals(LockType.X,lockType) ||
                  Objects.equals(LockType.IX,lockType) ||
                  Objects.equals(LockType.SIX ,lockType);
        };

        // get all children
        final List<ResourceName> descendants = getAllDescendants(transaction);
        if(descendants.isEmpty() || !prevLockType.isIntent()){
            return;
        }
        boolean isToX = toXPredicate.test(prevLockType);
        if(!isToX){
            isToX = descendants.stream()
                    .map(resourceName -> lockman.getLockType(transaction,resourceName))
                    .anyMatch(toXPredicate);
        }

        ResourceName curResourceName = getResourceName();
        descendants.add(curResourceName);
        updateChildLockNum(transaction,descendants,-1);
        if(isToX){
            lockman.acquireAndRelease(transaction,curResourceName,LockType.X,descendants);
        }else {
            lockman.acquireAndRelease(transaction,curResourceName,LockType.S,descendants);
        }
    }

    /**
     * get all descendants of the transactionContext
     * @param transactionContext
     * @return
     */
    private List<ResourceName> getAllDescendants(TransactionContext transactionContext){
        return getEligibleDescendants(transactionContext,
                lock -> lock.name.isDescendantOf(getResourceName()));
    }

    private List<ResourceName> getEligibleDescendants(TransactionContext transactionContext,Predicate<? super Lock> predicate){
        return lockman.getLocks(transactionContext)
                .stream()
                .filter(predicate)
                .map(lock -> lock.name)
                .collect(Collectors.toList());
    }
    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement

        return lockman.getLockType(transaction,getResourceName());
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType lockType = getExplicitLockType(transaction);
        if(!Objects.equals(lockType,LockType.NL)){
            return lockType;
        }
        LockContext parent = parentContext();
        if(parent != null){
            LockType parentLockType = parent.getEffectiveLockType(transaction);
            if(Objects.equals(LockType.SIX,parentLockType)){
                lockType = LockType.S;
            }
            if(!parentLockType.isIntent()){
                lockType = parentLockType;
            }
        }

        return lockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        for(LockContext parent = parentContext();parent!=null;parent = parent.parentContext()){
            LockType lockType = parent.getEffectiveLockType(transaction);
            if(Objects.equals(LockType.SIX,lockType)){
                return true;
            }
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        return getEligibleDescendants(transaction,lock -> (lock.name.isDescendantOf(getResourceName())) &&
                Objects.equals(LockType.S,lock.lockType) || Objects.equals(LockType.IS,lock.lockType));
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }

    private void checkReadOnly(){
        if(readonly){
            throw new UnsupportedOperationException("context is readonly");
        }
    }
}

