// --- Cloud Function Exports Hub ---
// Each CF is stored in /cf/*.js and exported here.

const functions = require("firebase-functions/v1");
const admin = require("firebase-admin");

// Initialize Admin SDK ONCE
admin.initializeApp();

// Export each CF from /cf folder
// (We'll add real ones here when we migrate them.)
//
// Example (commented until we add files):
// exports.writePayList = require("./cf/writePayList");
// exports.applyTotalPay = require("./cf/applyTotalPay");
// exports.runSimulation = require("./cf/runSimulation");

exports.cardComputeMinPymt = require("./cf/cardComputeMinPymt");
exports.cardComputeIsCurrent = require("./cf/cardComputeIsCurrent");
exports.loanComputeIsCurrent = require("./cf/loanComputeIsCurrent");
exports.consolidateUserStocks = require("./cf/consolidateUserStocks");
exports.cleanAndWriteUserCardRecs = require("./cf/cleanAndWriteUserCardRecs");
exports.writeCloseCandidates = require("./cf/writeCloseCandidates");
exports.writeAndComputePayPrioritiesList = require('./cf/writeAndComputePayPrioritiesList');
exports.writeAggStocksData = require('./cf/writeAggStocksData');
exports.transferOpenCloseMod = require('./cf/transferOpenCloseMod');
exports.computePaymentHistoryMetrics = require('./cf/computePaymentHistoryMetrics');
exports.computeAmountsOwedMetrics = require('./cf/computeAmountsOwedMetrics');
exports.computeLengthOfHistoryMetrics = require('./cf/computeLengthOfHistoryMetrics');
exports.computeCreditMixMetrics = require('./cf/computeCreditMixMetrics');
exports.writeUserLoanRecs = require('./cf/writeUserLoanRecs');
exports.deselectAllCardsAndLoans = require('./cf/deselectAllCardsAndLoans');
exports.applyMinPaymentsFromPayPriorityList = require('./cf/applyMinPaymentsFromPayPriorityList');
exports.undoMinPayFromPayPriorityList = require('./cf/undoMinPayFromPayPriorityList');
exports.applyTotalBudgetFromPayPriorityList = require('./cf/applyTotalBudgetFromPayPriorityList');
exports.undoTotalBudgetFromPayPriorityList = require('./cf/undoTotalBudgetFromPayPriorityList');
exports.applySingleDocPayFromPayPriorityList =
  require('./cf/applySingleDocPayFromPayPriorityList');
exports.undoSingleDocPayFromPayPriorityList =
  require('./cf/undoSingleDocPayFromPayPriorityList');
exports.orchestrateActionRecommendations =
  require('./cf/orchestrateActionRecommendations');
exports.writeAndComputePayPrioritiesListORCH =
  require('./cf/writeAndComputePayPrioritiesListORCH');
exports.writeOpenActionsListORCH =
  require('./cf/writeOpenActionsListORCH').writeOpenActionsListORCH;
exports.writeCloseActionsListORCH =
  require('./cf/writeCloseActionsListORCH').writeCloseActionsListORCH;
exports.writeUseCardsListOrch =
  require('./cf/writeUseCardsListOrch').writeUseCardsListOrch;
exports.applyUseEstimateSpendFromUseActions =
  require('./cf/applyUseEstimateSpendFromUseActions').applyUseEstimateSpendFromUseActions;
exports.simulatePlan =
  require('./cf/1 - simulatePlan').simulatePlan;
exports.testLoader =
  require('./cf/testLoader').testLoader;
exports.simVizOutput =
  require('./cf/simVizOutput').simVizOutput;
exports.computeAssumedMonthlyBudget =
  require('./cf/computeAssumedMonthlyBudget').computeAssumedMonthlyBudget;
exports.conservativeRiskParameters =
  require('./cf/conservativeRiskParameters').conservativeRiskParameters;
exports.aggressiveRiskParameters = require('./cf/aggressiveRiskParameters').aggressiveRiskParameters;
exports.uploadReport = require('./cf/uploadReport').uploadReport;
exports.validateReportFormat =
  require('./cf/validateReportFormat').validateReportFormat;
exports.stageFromUpload = require('./cf/stageFromUpload').stageFromUpload;
exports.originFromStage =
  require('./cf/originFromStage').originFromStage;
exports.stageFromOrigin =
  require('./cf/stageFromOrigin').stageFromOrigin;
exports.anonymousUserPrep =
  require('./cf/anonymousUserPrep').anonymousUserPrep;


// ----------------------------------------------------
// NEW — Auto-set isPaid = true when amount becomes 0
// From /cf/latesAndCollectionsIsPaid.js
// ----------------------------------------------------
const latesPaidCF = require("./cf/latesAndCollectionsIsPaid");

exports.setIsPaidOnCardLates = latesPaidCF.setIsPaidOnCardLates;
exports.setIsPaidOnLoanLates = latesPaidCF.setIsPaidOnLoanLates;
exports.setIsPaidOnCollections = latesPaidCF.setIsPaidOnCollections;
