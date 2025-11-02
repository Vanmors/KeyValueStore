package dev.kvstore.raft;

import dev.kvstore.core.model.WALEntry;

import java.util.List;

public final class RaftRpc {
    public record RequestVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
    }

    public record RequestVoteResponse(long term, boolean voteGranted) {
    }

    public record AppendEntriesRequest(
            long term,
            String leaderId,
            long prevLogIndex,
            long prevLogTerm,
            List<LogEntry> entries,
            long leaderCommit) {
    }

    public record AppendEntriesResponse(long term, boolean success, long matchIndex) {
    }

    public record LogEntry(long index, long term, WALEntry entry) {
    }
}
