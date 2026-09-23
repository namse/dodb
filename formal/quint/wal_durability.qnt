module wal_durability {
  type Frame = APage | ACommit | BPage1 | BPage2 | BCommit
  type LogicalCommit = CommitA | CommitB
  type Phase = Writing | ReadyToSync | Syncing | Synced | Failed | Crashed | Recovered | Returned

  pure val GroupFrames: List[Frame] = List(APage, ACommit, BPage1, BPage2, BCommit)
  pure val AllCommits: Set[LogicalCommit] = Set(CommitA, CommitB)
  pure def frameLsn(frame: Frame): int =
    match (frame) {
      | APage => 1
      | ACommit => 2
      | BPage1 => 3
      | BPage2 => 4
      | BCommit => 5
    }
  pure val CommitALsn = frameLsn(ACommit)
  pure val CommitBLsn = frameLsn(BCommit)

  var writeIndex: int
  var writtenFrames: List[Frame]
  var durableCount: int
  var tornTail: bool
  var syncStarted: bool
  var syncSucceeded: bool
  var appendReturnedSuccess: bool
  var broken: bool
  var recoveredCommits: Set[LogicalCommit]
  var recoveredNextLsn: int
  var phase: Phase
  var crashAfterSync: bool

  action init = all {
    writeIndex' = 0,
    writtenFrames' = List(),
    durableCount' = 0,
    tornTail' = false,
    syncStarted' = false,
    syncSucceeded' = false,
    appendReturnedSuccess' = false,
    broken' = false,
    recoveredCommits' = Set(),
    recoveredNextLsn' = 1,
    phase' = Writing,
    crashAfterSync' = false,
  }

  action WriteNextFrame = {
    val nextFrames = writtenFrames.append(GroupFrames.nth(writeIndex))
    all {
      phase == Writing,
      writeIndex < GroupFrames.length(),
      writtenFrames == GroupFrames.slice(0, writeIndex),
      writtenFrames' = nextFrames,
      writeIndex' = writeIndex + 1,
      durableCount' = durableCount,
      tornTail' = tornTail,
      syncStarted' = syncStarted,
      syncSucceeded' = syncSucceeded,
      appendReturnedSuccess' = appendReturnedSuccess,
      broken' = broken,
      recoveredCommits' = recoveredCommits,
      recoveredNextLsn' = recoveredNextLsn,
      phase' = if (writeIndex + 1 == GroupFrames.length()) ReadyToSync else Writing,
      crashAfterSync' = crashAfterSync,
    }
  }

  action SpontaneousPersistence = {
    nondet persistedCount = Set(0, 1, 2, 3, 4, 5).oneOf()
    nondet hasTornTail = Set(true, false).oneOf()
    all {
      phase == Writing or phase == ReadyToSync or phase == Syncing,
      not (syncSucceeded),
      persistedCount >= durableCount,
      persistedCount <= writtenFrames.length(),
      durableCount' = persistedCount,
      writeIndex' = writeIndex,
      writtenFrames' = writtenFrames,
      tornTail' = hasTornTail,
      syncStarted' = syncStarted,
      syncSucceeded' = syncSucceeded,
      appendReturnedSuccess' = appendReturnedSuccess,
      broken' = broken,
      recoveredCommits' = recoveredCommits,
      recoveredNextLsn' = recoveredNextLsn,
      phase' = phase,
      crashAfterSync' = crashAfterSync,
    }
  }

  action StartSync = all {
    phase == ReadyToSync,
    not (syncStarted),
    writeIndex == GroupFrames.length(),
    phase' = Syncing,
    syncStarted' = true,
    writeIndex' = writeIndex,
    writtenFrames' = writtenFrames,
    durableCount' = durableCount,
    tornTail' = tornTail,
    syncSucceeded' = syncSucceeded,
    appendReturnedSuccess' = appendReturnedSuccess,
    broken' = broken,
    recoveredCommits' = recoveredCommits,
    recoveredNextLsn' = recoveredNextLsn,
    crashAfterSync' = crashAfterSync,
  }

  action SyncSuccess = all {
    phase == Syncing,
    syncStarted,
    not (syncSucceeded),
    durableCount' = writtenFrames.length(),
    syncSucceeded' = true,
    phase' = Synced,
    writeIndex' = writeIndex,
    writtenFrames' = writtenFrames,
    tornTail' = false,
    syncStarted' = syncStarted,
    appendReturnedSuccess' = appendReturnedSuccess,
    broken' = broken,
    recoveredCommits' = recoveredCommits,
    recoveredNextLsn' = recoveredNextLsn,
    crashAfterSync' = crashAfterSync,
  }

  action SyncFailure = {
    nondet persistedCount = Set(0, 1, 2, 3, 4, 5).oneOf()
    nondet hasTornTail = Set(true, false).oneOf()
    all {
      phase == Syncing,
      syncStarted,
      not (syncSucceeded),
      persistedCount >= durableCount,
      persistedCount <= writtenFrames.length(),
      durableCount' = persistedCount,
      syncSucceeded' = false,
      phase' = Failed,
      broken' = true,
      tornTail' = hasTornTail,
      writeIndex' = writeIndex,
      writtenFrames' = writtenFrames,
      syncStarted' = syncStarted,
      appendReturnedSuccess' = appendReturnedSuccess,
      recoveredCommits' = recoveredCommits,
      recoveredNextLsn' = recoveredNextLsn,
      crashAfterSync' = crashAfterSync,
    }
  }

  action CrashDuringSync = all {
    phase == Syncing,
    syncStarted,
    not (syncSucceeded),
    phase' = Crashed,
    broken' = true,
    crashAfterSync' = false,
    writeIndex' = writeIndex,
    writtenFrames' = writtenFrames,
    durableCount' = durableCount,
    tornTail' = tornTail,
    syncStarted' = syncStarted,
    syncSucceeded' = syncSucceeded,
    appendReturnedSuccess' = appendReturnedSuccess,
    recoveredCommits' = recoveredCommits,
    recoveredNextLsn' = recoveredNextLsn,
  }

  action CrashBeforeSync = all {
    phase == Writing or phase == ReadyToSync,
    not (syncStarted),
    not (syncSucceeded),
    phase' = Crashed,
    broken' = true,
    crashAfterSync' = false,
    writeIndex' = writeIndex,
    writtenFrames' = writtenFrames,
    durableCount' = durableCount,
    tornTail' = tornTail,
    syncStarted' = syncStarted,
    syncSucceeded' = syncSucceeded,
    appendReturnedSuccess' = appendReturnedSuccess,
    recoveredCommits' = recoveredCommits,
    recoveredNextLsn' = recoveredNextLsn,
  }

  action CrashAfterSyncFailure = all {
    phase == Failed,
    not (syncSucceeded),
    phase' = Crashed,
    broken' = broken,
    crashAfterSync' = false,
    writeIndex' = writeIndex,
    writtenFrames' = writtenFrames,
    durableCount' = durableCount,
    tornTail' = tornTail,
    syncStarted' = syncStarted,
    syncSucceeded' = syncSucceeded,
    appendReturnedSuccess' = appendReturnedSuccess,
    recoveredCommits' = recoveredCommits,
    recoveredNextLsn' = recoveredNextLsn,
  }

  action ReturnSuccess = all {
    phase == Synced,
    syncSucceeded,
    not (broken),
    appendReturnedSuccess' = true,
    phase' = Returned,
    writeIndex' = writeIndex,
    writtenFrames' = writtenFrames,
    durableCount' = durableCount,
    tornTail' = tornTail,
    syncStarted' = syncStarted,
    syncSucceeded' = syncSucceeded,
    broken' = broken,
    recoveredCommits' = recoveredCommits,
    recoveredNextLsn' = recoveredNextLsn,
    crashAfterSync' = crashAfterSync,
  }

  action LoseReturnAndCrash = all {
    phase == Synced,
    syncSucceeded,
    not (appendReturnedSuccess),
    phase' = Crashed,
    broken' = true,
    crashAfterSync' = true,
    writeIndex' = writeIndex,
    writtenFrames' = writtenFrames,
    durableCount' = durableCount,
    tornTail' = tornTail,
    syncStarted' = syncStarted,
    syncSucceeded' = syncSucceeded,
    appendReturnedSuccess' = appendReturnedSuccess,
    recoveredCommits' = recoveredCommits,
    recoveredNextLsn' = recoveredNextLsn,
  }

  action Recover = {
    val commits = Set().union(if (durableCount >= CommitALsn) Set(CommitA) else Set())
      .union(if (durableCount >= CommitBLsn) Set(CommitB) else Set())
    all {
      phase == Crashed,
      recoveredCommits' = commits,
      recoveredNextLsn' = durableCount + 1,
      phase' = Recovered,
      writeIndex' = writeIndex,
      writtenFrames' = writtenFrames,
      durableCount' = durableCount,
      tornTail' = tornTail,
      syncStarted' = syncStarted,
      syncSucceeded' = syncSucceeded,
      appendReturnedSuccess' = appendReturnedSuccess,
      broken' = broken,
      crashAfterSync' = crashAfterSync,
    }
  }

  action terminalStutter = all {
    phase == Recovered,
    writeIndex' = writeIndex,
    writtenFrames' = writtenFrames,
    durableCount' = durableCount,
    tornTail' = tornTail,
    syncStarted' = syncStarted,
    syncSucceeded' = syncSucceeded,
    appendReturnedSuccess' = appendReturnedSuccess,
    broken' = broken,
    recoveredCommits' = recoveredCommits,
    recoveredNextLsn' = recoveredNextLsn,
    phase' = phase,
    crashAfterSync' = crashAfterSync,
  }

  action step = any {
    WriteNextFrame,
    SpontaneousPersistence,
    StartSync,
    SyncSuccess,
    SyncFailure,
    CrashBeforeSync,
    CrashDuringSync,
    CrashAfterSyncFailure,
    ReturnSuccess,
    LoseReturnAndCrash,
    Recover,
    terminalStutter,
  }

  val TypeOK = all {
    writeIndex >= 0,
    writeIndex <= GroupFrames.length(),
    writtenFrames == GroupFrames.slice(0, writeIndex),
    durableCount >= 0,
    durableCount <= writtenFrames.length(),
    phase == Writing or phase == ReadyToSync or phase == Syncing or phase == Synced or phase == Failed or phase == Crashed or phase == Recovered or phase == Returned,
    recoveredCommits.forall(commit => AllCommits.contains(commit)),
    recoveredNextLsn >= 1,
  }

  val FixedFramePrefixOrdering = writtenFrames == GroupFrames.slice(0, writeIndex)
  val DurablePrefixBound = durableCount <= writtenFrames.length()
  val CommitBRequiresCommitA = not (recoveredCommits.contains(CommitB)) or recoveredCommits.contains(CommitA)
  val CommitARecoveryMatchesLsn = (phase == Recovered and recoveredCommits.contains(CommitA)) == (phase == Recovered and durableCount >= CommitALsn)
  val CommitBRecoveryMatchesLsn = (phase == Recovered and recoveredCommits.contains(CommitB)) == (phase == Recovered and durableCount >= CommitBLsn)
  val RecoveryNextLsnMatchesPrefix = phase != Recovered or recoveredNextLsn == durableCount + 1
  val SyncSuccessMeansWholeGroupDurable = not (syncSucceeded) or durableCount == GroupFrames.length()
  val AppendSuccessRequiresSyncSuccess = not (appendReturnedSuccess) or syncSucceeded
  val SyncFailureNeverReturnsSuccess = phase != Failed or not (appendReturnedSuccess)
  val CrashBeforeSyncCannotClaimSuccess = not (phase == Crashed and not (crashAfterSync)) or not (syncSucceeded)
  val SuccessfulSyncRecoversWholeGroup = not (phase == Recovered and crashAfterSync) or recoveredCommits == AllCommits
  val SyncCannotRestart = not (syncStarted) or phase != ReadyToSync
  val NoSyncRetryAfterFailure = phase != Failed or syncStarted
  val LostReturnRetainsWholeGroup = not (phase == Recovered and crashAfterSync) or (syncSucceeded and recoveredCommits == AllCommits)
  val FailedClaimAllowsObservedRecovery = phase != Recovered or syncSucceeded or recoveredCommits == (Set().union(if (durableCount >= CommitALsn) Set(CommitA) else Set()).union(if (durableCount >= CommitBLsn) Set(CommitB) else Set()))
  val state_invariants = all {
    TypeOK,
    FixedFramePrefixOrdering,
    DurablePrefixBound,
    CommitBRequiresCommitA,
    CommitARecoveryMatchesLsn,
    CommitBRecoveryMatchesLsn,
    RecoveryNextLsnMatchesPrefix,
    SyncSuccessMeansWholeGroupDurable,
    AppendSuccessRequiresSyncSuccess,
    SyncFailureNeverReturnsSuccess,
    CrashBeforeSyncCannotClaimSuccess,
    SuccessfulSyncRecoversWholeGroup,
    NoSyncRetryAfterFailure,
    SyncCannotRestart,
    LostReturnRetainsWholeGroup,
    FailedClaimAllowsObservedRecovery,
  }
}
