package com.sun.jini.mahalo;

public class PHolder {
    private ParticipantHandle participantHandle;
    private int partitionId;
    private String clusterName;

    public PHolder() {
    }

    public PHolder(ParticipantHandle participantHandle) {
        this.participantHandle = participantHandle;
        this.partitionId = participantHandle.getPartionId();
        this.clusterName = participantHandle.getClusterName();
    }

    public ParticipantHandle getParticipantHandle() {
        return participantHandle;
    }

    public void setParticipantHandle(ParticipantHandle participantHandle) {
        this.participantHandle = participantHandle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PHolder pHolder = (PHolder) o;

        if (partitionId != pHolder.partitionId) return false;
        return clusterName != null ? clusterName.equals(pHolder.clusterName) : pHolder.clusterName == null;
    }

    @Override
    public int hashCode() {
        int result = partitionId;
        result = 31 * result + (clusterName != null ? clusterName.hashCode() : 0);
        return result;
    }

    //
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//
//        PHolder pHolder = (PHolder) o;
//
//        if (pHolder.participantHandle == null) return false;
//
//        ParticipantHandle thatParticipantHandle = pHolder.participantHandle;
//
//        if (participantHandle != null &&
//                (participantHandle.getClusterName().equals(thatParticipantHandle.getClusterName())) &&
//                (participantHandle.getPartionId() ==  thatParticipantHandle.getPartionId())
//                ) {
//            return true;
//        }
//
//        return false;
//    }
}
