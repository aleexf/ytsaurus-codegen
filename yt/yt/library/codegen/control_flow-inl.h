#ifndef CONTROL_FLOW_INL_H_
#error "Direct inclusion of this file is not allowed, include control_flow.h"
// For the sake of sane code completion.
#include "control_flow.h"
#endif
#undef CONTROL_FLOW_INL_H_

namespace NYT::NCodegen {

/////////////////////////////////////////////////////////////////////////////

template <CIRBuilder TBuilder>
TControlFlowBuilderBase<TBuilder>::TControlFlowBuilderBase(TBuilder& builder, std::vector<llvm::Value**> trackedVariables)
    : Builder_(builder)
    , TrackedVariables_(std::move(trackedVariables))
{
    DumpVariablesValues();
}

template <CIRBuilder TBuilder>
void TControlFlowBuilderBase<TBuilder>::CreatePHIValues()
{
    for (size_t index = 0; index < TrackedVariables_.size(); ++index) {
        auto** var = TrackedVariables_[index];
        auto* phi = Builder_.CreatePHI((*var)->getType(), VariablesValues_.size());
        for (auto& [basicBlock, variablesValues] : VariablesValues_) {
            phi->addIncoming(variablesValues[index], basicBlock);
        }
    }
}

template <CIRBuilder TBuilder>
void TControlFlowBuilderBase<TBuilder>::DumpVariablesValues()
{
    auto currentBB = Builder_.GetInsertBlock();

    if (!FinalVariablesValues_.empty()) {
        // Add incoming to existing PHI instructions.
        for (size_t index = 0; index < TrackedVariables_.size(); ++index) {
            if (*TrackedVariables_[index] != FinalVariablesValues_[index]) {
                FinalVariablesValues_[index]->addIncoming(*TrackedVariables_[index], currentBB);
            }
        }
        return;
    }

    VariablesValues_.emplace_back(currentBB, std::vector<llvm::Value*>());
    VariablesValues_.back().second.reserve(TrackedVariables_.size());
    for (auto** variable : TrackedVariables_) {
        VariablesValues_.back().second.push_back(*variable);
    }
}

template <CIRBuilder TBuilder>
void TControlFlowBuilderBase<TBuilder>::RestoreInitialVariablesValues()
{
    YT_VERIFY(!VariablesValues_.empty());
    for (size_t index = 0; index < TrackedVariables_.size(); ++index) {
        *TrackedVariables_[index] = VariablesValues_[0].second[index];
    }
}

template <CIRBuilder TBuilder>
void TControlFlowBuilderBase<TBuilder>::RestoreFinalVariablesValues()
{
    for (size_t index = 0; index < TrackedVariables_.size(); ++index) {
        *TrackedVariables_[index] = FinalVariablesValues_[index];
    }
}

/////////////////////////////////////////////////////////////////////////////

template <CIRBuilder TBuilder>
TIfBuilder<TBuilder>::TIfBuilder(
    TBuilder& builder,
    llvm::Value* condition,
    std::vector<llvm::Value**> trackedVariables)
    : TControlFlowBuilderBase<TBuilder>(builder, std::move(trackedVariables))
    , ThenBB_(Builder_.CreateBB("if.then"))
    , ElseBB_(Builder_.CreateBB("if.else"))
    , AfterBB_(Builder_.CreateBB("if.after"))
{
    Builder_.CreateCondBr(condition, ThenBB_, ElseBB_);
}

template <CIRBuilder TBuilder>
TIfBuilder<TBuilder>::~TIfBuilder()
{
    if (ThenBB_->empty()) {
        Builder_.SetInsertPoint(ThenBB_);
        Builder_.CreateBr(AfterBB_);
    }

    if (ElseBB_->empty()) {
        Builder_.SetInsertPoint(ElseBB_);
        Builder_.CreateBr(AfterBB_);
    }

    Builder_.SetInsertPoint(AfterBB_);
    this->CreatePHIValues();
    this->RestoreFinalVariablesValues();
}

template <CIRBuilder TBuilder>
TIfBuilder<TBuilder>& TIfBuilder<TBuilder>::Then(TBodyBuilder thenBodyBuilder)
{
    YT_VERIFY(ThenBB_->empty());

    this->RestoreInitialVariablesValues();

    Builder_.SetInsertPoint(ThenBB_);
    thenBodyBuilder(Builder_);
    Builder_.CreateBr(AfterBB_);

    this->DumpVariablesValues();

    return *this;
}

template <CIRBuilder TBuilder>
TIfBuilder<TBuilder>& TIfBuilder<TBuilder>::Else(TBodyBuilder elseBodyBuilder)
{
    YT_VERIFY(ElseBB_->empty());

    this->RestoreInitialVariablesValues();

    Builder_.SetInsertPoint(ElseBB_);
    elseBodyBuilder(Builder_);
    Builder_.CreateBr(AfterBB_);

    this->DumpVariablesValues();

    return *this;
}

/////////////////////////////////////////////////////////////////////////////

template <CIRBuilder TBuilder>
TSwitchBuilder<TBuilder>::TSwitchBuilder(TBuilder& builder, llvm::Value* value, std::vector<llvm::Value**> trackedVariables)
    : TControlFlowBuilderBase<TBuilder>(builder, std::move(trackedVariables))
    , Value_(value)
    , AfterBB_(Builder_.CreateBB("switch.after"))
    , DefaultBB_(Builder_.CreateBB("switch.default"))
    , SwitchInstance_(Builder_.CreateSwitch(Value_, DefaultBB_))
{ }

template <CIRBuilder TBuilder>
TSwitchBuilder<TBuilder>::~TSwitchBuilder()
{
    if (DefaultBB_->empty()) {
        Builder_.SetInsertPoint(DefaultBB_);
        Builder_.CreateBr(AfterBB_);
    }

    Builder_.SetInsertPoint(AfterBB_);
    this->CreatePHIValues();
    this->RestoreFinalVariablesValues();
}

template <CIRBuilder TBuilder>
TSwitchBuilder<TBuilder>& TSwitchBuilder<TBuilder>::Case(llvm::ConstantInt* value, TBodyBuilder caseBodyBuilder)
{
    return Case(std::vector{value}, std::move(caseBodyBuilder));
}

template <CIRBuilder TBuilder>
TSwitchBuilder<TBuilder>& TSwitchBuilder<TBuilder>::Case(std::vector<llvm::ConstantInt*> values, TBodyBuilder caseBodyBuilder)
{
    this->RestoreInitialVariablesValues();

    auto* bodyBlock = Builder_.CreateBB("switch.case");
    Builder_.SetInsertPoint(bodyBlock);
    caseBodyBuilder(Builder_);
    Builder_.CreateBr(AfterBB_);

    this->DumpVariablesValues();

    for (auto* value : values) {
        SwitchInstance_->addCase(value, bodyBlock);
    }

    return *this;
}

template <CIRBuilder TBuilder>
TSwitchBuilder<TBuilder>& TSwitchBuilder<TBuilder>::Default(TBodyBuilder defaultBodyBuilder)
{
    YT_VERIFY(DefaultBB_->empty());

    this->RestoreInitialVariablesValues();

    Builder_.SetInsertPoint(DefaultBB_);
    defaultBodyBuilder(Builder_);
    Builder_.CreateBr(AfterBB_);

    this->DumpVariablesValues();

    return *this;
}

/////////////////////////////////////////////////////////////////////////////

template <CIRBuilder TBuilder>
TWhileBuilder<TBuilder>::TWhileBuilder(TBuilder& builder, std::vector<llvm::Value**> trackedVariables)
    : TControlFlowBuilderBase<TBuilder>(builder, std::move(trackedVariables))
    , AfterBB_(Builder_.CreateBB("while.after"))
    , ConditionBB_(Builder_.CreateBB("while.condition"))
    , BodyBB_(Builder_.CreateBB("while.body"))
{
    Builder_.CreateBr(ConditionBB_);

    this->CreatePHIValues();
}

template <CIRBuilder TBuilder>
TWhileBuilder<TBuilder>::~TWhileBuilder()
{
    YT_VERIFY(!ConditionBB_->empty() && !BodyBB_->empty());

    Builder_.SetInsertPoint(AfterBB_);
    this->RestoreFinalVariablesValues();
}

template <CIRBuilder TBuilder>
TWhileBuilder<TBuilder>& TWhileBuilder<TBuilder>::Condition(TConditionBuilder conditionBuilder)
{
    YT_VERIFY(ConditionBB_->empty());

    this->RestoreFinalVariablesValues();

    Builder_.SetInsertPoint(ConditionBB_);
    auto* condition = conditionBuilder(Builder_);
    Builder_.CreateCondBr(condition, BodyBB_, AfterBB_);

    return *this;
}

template <CIRBuilder TBuilder>
TWhileBuilder<TBuilder>& TWhileBuilder<TBuilder>::Body(TBodyBuilder bodyBuilder)
{
    YT_VERIFY(BodyBB_->empty());

    this->RestoreFinalVariablesValues();

    Builder_.SetInsertPoint(BodyBB_);
    bodyBuilder(Builder_);
    Builder_.CreateBr(ConditionBB_);

    this->DumpVariablesValues();

    return *this;
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
