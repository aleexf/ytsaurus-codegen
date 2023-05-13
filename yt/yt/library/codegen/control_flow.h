#pragma once

#include "builder_base.h"

#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/IRBuilder.h>
#include <climits>

namespace NYT::NCodegen {

/////////////////////////////////////////////////////////////////////////////

/// This file contains helpers which are used for building control flow
/// in the high-level programming languages mannear.
/// Every builder constructor has `trakedVariables` as its optional argument.
/// For each tracked variable will be created a PHI instruction, that'll capture
/// its value from each statement block.
/// For better understanding see example below:
///
/// \code{.cpp}
///
/// llvm::Value* ThreeXPlusOneProblem(TIRBuilderBase& builder, llvm::Value* x) {
///     llvm::Value* result;
///     auto* leastBit = builder.CreateAnd(x, builder.ConstantIntValue<int64_t>(1));
///     auto* isEven = builder.CreateICmpSLT(leastBit, builder.ConstantIntValue<int64_t>(0));
///
///     TIfBuilder(builder, isEven, {&result})
///         .Then([&result, x](auto& builder) {
///             result = builder.CreateSDiv(x, builder.ConstantIntValue<int64_t>(2));
///         })
///         .Else([&result, x](auto& builder) {
///             result = builder.CreateMul(x, builder.ConstantIntValue<int64_t>(3));
///             result = builder.CreateAdd(result, builder.ConstantIntValue<int64_t>(1));
///         });
///
///     return result;
/// }
///
/// \endcode

template <class T>
concept CIRBuilder = std::is_base_of_v<TIRBuilderBase, T>;

template <CIRBuilder TBuilder>
class TControlFlowBuilderBase
{
protected:
    using TBodyBuilder = std::function<void(TBuilder&)>;

    explicit TControlFlowBuilderBase(TBuilder& builder, std::vector<llvm::Value**> trackedVariables = {});

    void CreatePHIValues();

    void DumpVariablesValues();

    void RestoreInitialVariablesValues();

    void RestoreFinalVariablesValues();

protected:
    TBuilder& Builder_;

private:
    std::vector<llvm::Value**> TrackedVariables_;
    std::vector<llvm::PHINode*> FinalVariablesValues_;
    std::vector<std::pair<llvm::BasicBlock*, std::vector<llvm::Value*>>> VariablesValues_;
};

/////////////////////////////////////////////////////////////////////////////

template <CIRBuilder TBuilder>
class [[nodiscard]] TIfBuilder
    : public TControlFlowBuilderBase<TBuilder>
{
private:
    using TControlFlowBuilderBase<TBuilder>::Builder_;

public:
    using typename TControlFlowBuilderBase<TBuilder>::TBodyBuilder;

    TIfBuilder(TBuilder& builder, llvm::Value* condition, std::vector<llvm::Value**> trackedVariables = {});

    ~TIfBuilder();

    TIfBuilder& Then(TBodyBuilder thenBodyBuilder);

    TIfBuilder& Else(TBodyBuilder elseBodyBuilder);

private:
    llvm::BasicBlock* ThenBB_;
    llvm::BasicBlock* ElseBB_;
    llvm::BasicBlock* AfterBB_;
};

/////////////////////////////////////////////////////////////////////////////

template <CIRBuilder TBuilder>
class [[nodiscard]] TSwitchBuilder
    : public TControlFlowBuilderBase<TBuilder>
{
private:
    using TControlFlowBuilderBase<TBuilder>::Builder_;

public:
    using typename TControlFlowBuilderBase<TBuilder>::TBodyBuilder;

    TSwitchBuilder(TBuilder& builder, llvm::Value* value, std::vector<llvm::Value**> trackedVariables = {});

    ~TSwitchBuilder();

    TSwitchBuilder& Case(llvm::ConstantInt* value, TBodyBuilder caseBodyBuilder);

    TSwitchBuilder& Case(std::vector<llvm::ConstantInt*> values, TBodyBuilder caseBodyBuilder);

    TSwitchBuilder& Default(TBodyBuilder defaultBodyBuilder);

private:
    llvm::Value* Value_;

    llvm::BasicBlock* AfterBB_;
    llvm::BasicBlock* DefaultBB_;

    llvm::SwitchInst* SwitchInstance_;
};

/////////////////////////////////////////////////////////////////////////////

template <CIRBuilder TBuilder>
class [[nodiscard]] TWhileBuilder
    : public TControlFlowBuilderBase<TBuilder>
{
private:
    using TControlFlowBuilderBase<TBuilder>::Builder_;

public:
    using typename TControlFlowBuilderBase<TBuilder>::TBodyBuilder;
    using TConditionBuilder = std::function<llvm::Value*(TBuilder&)>;

    explicit TWhileBuilder(TBuilder& builder, std::vector<llvm::Value**> trackedVariables = {});

    ~TWhileBuilder();

    /// NOTE: Condition block cannot modify tracked variables.
    TWhileBuilder& Condition(TConditionBuilder conditionBuilder);

    TWhileBuilder& Body(TBodyBuilder bodyBuilder);

private:
    llvm::BasicBlock* AfterBB_;
    llvm::BasicBlock* ConditionBB_;
    llvm::BasicBlock* BodyBB_;
};

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen

#define CONTROL_FLOW_INL_H_
#include "control_flow-inl.h"
#undef CONTROL_FLOW_INL_H_
