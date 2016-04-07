#include "artdaq/RTIDDS/RTIDDS.hh"

#include "messagefacility/MessageLogger/MessageLogger.h"

artdaq::RTIDDS::RTIDDS(std::string name, IOType iotype, std::string max_size) :
  name_(name),
  iotype_(iotype),
  max_size_(max_size)
{
  
  DDS_ReturnCode_t retcode = DDS_RETCODE_ERROR;
  DDS_DomainParticipantQos participant_qos;

  retcode = DDSDomainParticipantFactory::get_instance()->get_default_participant_qos(participant_qos);

  if (retcode != DDS_RETCODE_OK) {
    mf::LogWarning(name_) << "Problem obtaining default participant QoS, retcode was " << retcode;
  }

  retcode = DDSPropertyQosPolicyHelper::add_property (
                                                      participant_qos.property, "dds.builtin_type.octets.max_size",
                                                      max_size_.c_str(),
                                                      DDS_BOOLEAN_FALSE);

  if (retcode != DDS_RETCODE_OK) {
    mf::LogWarning(name_) << "Problem setting dds.builtin_type.octets.max_size, retcode was " << retcode;
  }

  participant_.reset( DDSDomainParticipantFactory::get_instance()->
                      create_participant(
                                         0,                              // Domain ID                                                
                                         participant_qos,
                                         nullptr,                           // Listener                                              
                                         DDS_STATUS_MASK_NONE)
                      );

  topic_octets_ = participant_->create_topic(
                                             "artdaq fragments",                        // Topic name                                
                                             DDSOctetsTypeSupport::get_type_name(), // Type name                                     
                                             DDS_TOPIC_QOS_DEFAULT,                 // Topic QoS                                     
                                             nullptr,                                  // Listener                                   
                                             DDS_STATUS_MASK_NONE) ;

  if (participant_ == nullptr || topic_octets_ == nullptr) {
    mf::LogWarning(name_) << "Problem setting up the RTI-DDS participant and/or topic";
  }

  // JCF, 9/16/15                                                                                                                    

  // Following effort to increase the max DDS buffer size from its                                                                   
  // default of 2048 bytes is cribbed from section 3.2.7 of the Core                                                                 
  // Utilities user manual, "Managing Memory for Built-in Types"                                                                     


  DDS_DataWriterQos writer_qos;

  retcode = participant_->get_default_datawriter_qos(writer_qos);

  if (retcode != DDS_RETCODE_OK) {
    mf::LogWarning(name_) << "Problem obtaining default datawriter QoS, retcode was " << retcode;
  }

  retcode = DDSPropertyQosPolicyHelper::add_property (
						      writer_qos.property, "dds.builtin_type.octets.alloc_size",
						      max_size_.c_str(),
						      DDS_BOOLEAN_FALSE);

  if (retcode != DDS_RETCODE_OK) {
    mf::LogWarning(name_) << "Problem setting dds.builtin_type.octets.alloc_size, retcode was " << retcode;
  }


  if (iotype_ == IOType::writer) {

    octets_writer_ = DDSOctetsDataWriter::narrow( participant_->create_datawriter(
                                                                                  topic_octets_,
                                                                                  writer_qos,
                                                                                  nullptr,                           // Listener   
										  DDS_STATUS_MASK_NONE)
                                                  );

    if (octets_writer_ == nullptr) {
      mf::LogWarning(name_) << "Problem setting up the RTI-DDS writer objects";
    }

  } else {

    octets_reader_ = participant_->create_datareader(
                                                     topic_octets_,
                                                     DDS_DATAREADER_QOS_DEFAULT,    // QoS                                           
                                                     &octets_listener_,                      // Listener                             
                                                     DDS_DATA_AVAILABLE_STATUS);

    if (octets_reader_ == nullptr) {
      mf::LogWarning(name_) << "Problem setting up the RTI-DDS reader objects";
    }

  }
}
