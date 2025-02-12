#pragma once

#include "job.h"
#include "public.h"

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TMasterJobSensors
{
    NProfiling::TCounter AdaptivelyRepairedChunksCounter;
    NProfiling::TCounter TotalRepairedChunksCounter;
    NProfiling::TCounter FailedRepairChunksCounter;
};

////////////////////////////////////////////////////////////////////////////////

class TMasterJobBase
    : public NJobAgent::TResourceHolder
    , public TRefCounted
{
public:
    DEFINE_SIGNAL(void(const NNodeTrackerClient::NProto::TNodeResources& resourcesDelta), ResourcesUpdated);
    DEFINE_SIGNAL(void(), JobPrepared);
    DEFINE_SIGNAL(void(), JobFinished);

public:
    TMasterJobBase(
        NJobTrackerClient::TJobId jobId,
        const NJobTrackerClient::NProto::TJobSpec& jobSpec,
        TString jobTrackerAddress,
        const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        IBootstrap* bootstrap);

    void Start();

    bool IsStarted() const noexcept;

    TResourceHolder* AsResourceHolder() noexcept;

    void Abort(const TError& error);

    NJobTrackerClient::TJobId GetId() const;

    NJobAgent::EJobType GetType() const;

    bool IsUrgent() const;

    const TString& GetJobTrackerAddress() const;

    NJobAgent::EJobState GetState() const;

    const NNodeTrackerClient::NProto::TNodeResources& GetResourceUsage() const;

    NJobTrackerClient::NProto::TJobResult GetResult() const;

    void BuildOrchid(NYTree::TFluentMap /*fluent*/) const;

    TInstant GetStartTime() const;

protected:
    const NJobTrackerClient::TJobId JobId_;
    const NJobTrackerClient::NProto::TJobSpec JobSpec_;
    const TString JobTrackerAddress_;
    const TDataNodeConfigPtr Config_;
    const TInstant StartTime_;
    IBootstrap* const Bootstrap_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    bool Started_ = false;

    NJobAgent::EJobState JobState_ = NJobAgent::EJobState::Waiting;

    TFuture<void> JobFuture_;

    NJobTrackerClient::NProto::TJobResult Result_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    virtual void DoRun() = 0;

    void GuardedRun();

    void SetCompleted();

    void SetFailed(const TError& error);

    void SetAborted(const TError& error);

    IChunkPtr FindLocalChunk(TChunkId chunkId, int mediumIndex);

    IChunkPtr GetLocalChunkOrThrow(TChunkId chunkId, int mediumIndex);

private:
    void DoSetFinished(NJobAgent::EJobState finalState, const TError& error);

    void OnResourcesAcquired() override;
};

DEFINE_REFCOUNTED_TYPE(TMasterJobBase)

////////////////////////////////////////////////////////////////////////////////

TMasterJobBasePtr CreateJob(
    NJobTrackerClient::TJobId jobId,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec,
    TString jobTrackerAddress,
    const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
    TDataNodeConfigPtr config,
    IBootstrap* bootstrap,
    const TMasterJobSensors& sensors);

////////////////////////////////////////////////////////////////////////////////

using TJobFactory = TCallback<TMasterJobBasePtr(
    NJobTrackerClient::TJobId jobId,
    const TString& jobTrackerAddress,
    const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec)>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
