#include "artdaq/TransferPlugins/TransferInterface.hh"

int artdaq::TransferInterface::my_rank = 0;

artdaq::TransferInterface::TransferInterface(const fhicl::ParameterSet& ps, Role role)
  : role_(role)
  , source_rank_(ps.get<int>("source_rank",my_rank))
  , destination_rank_(ps.get<int>("destination_rank", my_rank))
  , unique_label_(ps.get<std::string>("unique_label", "transfer_between_" + std::to_string(source_rank_) + "_and_" + std::to_string(destination_rank_)))
{
    mf::LogDebug( uniqueLabel() ) << "TransferInterface constructor has " << ps.to_string();
}
