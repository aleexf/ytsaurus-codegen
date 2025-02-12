#pragma once

#include "cube.h"
#include "tag_registry.h"
#include "sensor.h"

#include <yt/yt/library/profiling/tag.h>
#include <yt/yt/library/profiling/solomon/sensor_dump.pb.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/misc/intrusive_ptr.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCounterState)

struct TCounterState final
{
    TCounterState(
        TWeakPtr<TRefCounted> owner,
        std::function<i64()> reader,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Owner(std::move(owner))
        , Reader(std::move(reader))
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TWeakPtr<TRefCounted> Owner;
    const std::function<i64()> Reader;
    i64 LastValue = 0;

    TTagIdList TagIds;
    const TProjectionSet Projections;
};

DEFINE_REFCOUNTED_TYPE(TCounterState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTimeCounterState)

struct TTimeCounterState final
{
    TTimeCounterState(
        TWeakPtr<ITimeCounterImpl> owner,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Owner(std::move(owner))
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TWeakPtr<ITimeCounterImpl> Owner;
    TDuration LastValue = TDuration::Zero();

    TTagIdList TagIds;
    const TProjectionSet Projections;
};

DEFINE_REFCOUNTED_TYPE(TTimeCounterState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TGaugeState)

struct TGaugeState final
{
    TGaugeState(
        TWeakPtr<TRefCounted> owner,
        std::function<double()> reader,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Owner(std::move(owner))
        , Reader(std::move(reader))
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TWeakPtr<TRefCounted> Owner;
    const std::function<double()> Reader;

    TTagIdList TagIds;
    const TProjectionSet Projections;
};

DEFINE_REFCOUNTED_TYPE(TGaugeState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSummaryState)

struct TSummaryState final
{
    TSummaryState(
        TWeakPtr<ISummaryImpl> owner,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Owner(std::move(owner))
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TWeakPtr<ISummaryImpl> Owner;

    TTagIdList TagIds;
    const TProjectionSet Projections;
};

DEFINE_REFCOUNTED_TYPE(TSummaryState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTimerSummaryState)

struct TTimerSummaryState final
{
    TTimerSummaryState(
        TWeakPtr<ITimerImpl> owner,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Owner(owner)
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TWeakPtr<ITimerImpl> Owner;

    TTagIdList TagIds;
    const TProjectionSet Projections;
};

DEFINE_REFCOUNTED_TYPE(TTimerSummaryState)

////////////////////////////////////////////////////////////////////////////////


DECLARE_REFCOUNTED_STRUCT(THistogramState)

struct THistogramState final
{
    THistogramState(
        TWeakPtr<THistogram> owner,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Owner(owner)
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TWeakPtr<THistogram> Owner;

    TTagIdList TagIds;
    const TProjectionSet Projections;
};

DEFINE_REFCOUNTED_TYPE(THistogramState)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESensorType,
    ((Counter)        (1))
    ((TimeCounter)    (2))
    ((Gauge)          (3))
    ((Summary)        (4))
    ((Timer)          (5))
    ((Histogram)      (6))
    ((GaugeHistogram) (7))
);

////////////////////////////////////////////////////////////////////////////////

class TSensorSet
{
public:
    TSensorSet(
        TSensorOptions options,
        i64 iteration,
        int windowSize,
        int gridFactor);

    bool IsEmpty() const;

    void Profile(const TProfiler& profiler);
    void ValidateOptions(TSensorOptions options);

    void AddCounter(TCounterStatePtr counter);
    void AddGauge(TGaugeStatePtr gauge);
    void AddSummary(TSummaryStatePtr summary);
    void AddTimerSummary(TTimerSummaryStatePtr timer);
    void AddTimeCounter(TTimeCounterStatePtr counter);
    void AddHistogram(THistogramStatePtr counter);
    void AddGaugeHistogram(THistogramStatePtr counter);

    void RenameDynamicTag(const TDynamicTagPtr& dynamicTag, TTagId newTag);

    int Collect();

    void ReadSensors(
        const TString& name,
        const TReadOptions& options,
        TTagWriter* tagWriter,
        ::NMonitoring::IMetricConsumer* consumer) const;

    int ReadSensorValues(
        const TTagIdList& tagIds,
        int index,
        const TReadOptions& options,
        const TTagRegistry& tagRegistry,
        NYTree::TFluentAny fluent) const;

    void DumpCube(NProto::TCube* cube) const;

    int GetGridFactor() const;
    int GetObjectCount() const;
    int GetCubeSize() const;
    const TError& GetError() const;
    std::optional<ESensorType> GetType() const;

private:
    friend class TRemoteRegistry;

    const TSensorOptions Options_;
    const int GridFactor_;

    TError Error_;

    THashSet<TCounterStatePtr> Counters_;
    TCube<i64> CountersCube_;

    THashSet<TTimeCounterStatePtr> TimeCounters_;
    TCube<TDuration> TimeCountersCube_;

    THashSet<TGaugeStatePtr> Gauges_;
    TCube<double> GaugesCube_;

    THashSet<TSummaryStatePtr> Summaries_;
    TCube<TSummarySnapshot<double>> SummariesCube_;

    THashSet<TTimerSummaryStatePtr> Timers_;
    TCube<TSummarySnapshot<TDuration>> TimersCube_;

    THashSet<THistogramStatePtr> Histograms_;
    TCube<THistogramSnapshot> HistogramsCube_;

    THashSet<THistogramStatePtr> GaugeHistograms_;
    TCube<TGaugeHistogramSnapshot> GaugeHistogramsCube_;

    std::optional<ESensorType> Type_;
    TGauge CubeSize_;
    TGauge SensorsEmitted_;

    void OnError(TError error);

    void InitializeType(ESensorType type);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
