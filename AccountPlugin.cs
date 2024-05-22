using System;
using System.Collections.Generic;
using System.Reflection;
using System.ServiceModel;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace Korus.DPD.Armadillo.Plugins
{
    public class AccountPlugin : PluginBase
    {
        private readonly string postImageAlias = "PostImage";
        private readonly string preImageAlias = "PreImage";

        public AccountPlugin(string unsecure, string secure)
            : base(typeof(AccountPlugin))
        {
        }

        protected override void ExecuteCrmPlugin(LocalPluginContext localContext)
        {
            IPluginExecutionContext context = localContext.PluginExecutionContext;
            IOrganizationService service = localContext.OrganizationService;

            if (context.Stage == 10)
            {
                if (context.MessageName == "Merge")
                {
                    Merge_PreValidation(context, service);
                }
            }
            else if (context.Stage == 20)
            {
                if (context.MessageName == "Update")
                {
                    Update_PreOperation(context, service);
                }
            }
            else if (context.Stage == 40)
            {
                if (context.MessageName == "Update")
                {
                    Update_PostOperation(context, service);
                }
                else if (context.MessageName == "Assign")
                {
                    Assign_PostOperation(context, service);
                }
                else if (context.MessageName == "Create")
                {
                    Create_PostOperation(context, service);
                }
            }
        }

        private void Create_PostOperation(IPluginExecutionContext context, IOrganizationService service)
        {
            try
            {
                Entity targetEntity = (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity) ? (Entity)context.InputParameters["Target"] : null;

                if (targetEntity != null)
                {
                    if (targetEntity.Contains("new_leadsid") && targetEntity["new_leadsid"] != null && targetEntity["new_leadsid"].ToString() == "0")
                    {
                        var listRef = GetList(service);

                        if (listRef != null)
                        {
                            #region ADD Account to List
                            AddMemberListRequest req = new AddMemberListRequest();
                            req.ListId = listRef.Id;
                            req.EntityId = targetEntity.Id;

                            service.Execute(req);
                            #endregion

                            var campaignActivity = GetCampaignActivity(service, "ts_self_reged");
                            if (campaignActivity != null)
                            {
                                Entity phoneCreate = new Entity("phonecall");
                                phoneCreate["subject"] = "Прозвон нового клиента";

                                var fromEC = new EntityCollection();
                                Entity from = new Entity("activityparty");
                                from["partyid"] = GetOwnerUser(service, (EntityReference)targetEntity["ownerid"]);
                                fromEC.Entities.Add(from);
                                
                                phoneCreate["from"] = fromEC;

                                var toEC = new EntityCollection();
                                Entity to = new Entity("activityparty");
                                to["partyid"] = targetEntity.ToEntityReference();
                                toEC.Entities.Add(to);

                                phoneCreate["to"] = toEC;

                                if (targetEntity.Contains("telephone1") && targetEntity["telephone1"] != null)
                                {
                                    phoneCreate["phonenumber"] = targetEntity["telephone1"].ToString();
                                }

                                phoneCreate["directioncode"] = true;
                                phoneCreate["regardingobjectid"] = campaignActivity.ToEntityReference();
                                phoneCreate["ownerid"] = GetOwnerUser(service, (EntityReference)targetEntity["ownerid"]);
                                phoneCreate["actualdurationminutes"] = 30;
                                phoneCreate["scheduledend"] = DateTime.Now.AddDays(1);

                                phoneCreate.Id = service.Create(phoneCreate);

                                Entity campaignActivityUpdate = new Entity(campaignActivity.LogicalName, campaignActivity.Id);
                                campaignActivityUpdate["statuscode"] = new OptionSetValue(100000001);
                                campaignActivityUpdate["statecode"] = new OptionSetValue(0);

                                service.Update(campaignActivityUpdate);
                            }
                        }
                    }

                    #region Заполнение Территории продаж из Интереса
                    var new_leadsid = targetEntity.Contains("new_leadsid")
                        ? targetEntity.GetAttributeValue<string>("new_leadsid")
                        : null;
                    if (!String.IsNullOrEmpty(new_leadsid))
                    {
                        Entity lead = GetLeadByLeadNumber(service, new_leadsid);
                        if (lead != null)
                        {
                            Entity accountUpdate = new Entity(targetEntity.LogicalName, targetEntity.Id);
                            if (lead.Contains("ownerid"))
                            {
                                var ownerRef = lead.GetAttributeValue<EntityReference>("ownerid");                                
                                if (ownerRef.LogicalName == "systemuser")
                                {
                                    QueryExpression queryTer = new QueryExpression("new_territory");
                                    queryTer.NoLock = true;
                                    queryTer.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                                    queryTer.LinkEntities.Add(new LinkEntity("new_territory", "new_systemuser_new_territory_kurator", "new_territoryid", "new_territoryid", JoinOperator.Inner));
                                    queryTer.LinkEntities[0].LinkCriteria.AddCondition("systemuserid", ConditionOperator.Equal, ownerRef.Id);

                                    DataCollection<Entity> territories = service.RetrieveMultiple(queryTer).Entities;
                                    if (territories != null && territories.Count > 0)
                                    {
                                        accountUpdate["new_terrytoryid"] = territories[0].ToEntityReference();
                                    }
                                }
                                accountUpdate["ownerid"] = ownerRef;
                            }
                            if (lead.Contains("new_leadnumber"))
                            {
                                var leadNumber = lead.GetAttributeValue<string>("new_leadnumber");
                                if (!String.IsNullOrEmpty(leadNumber))
                                {
                                    accountUpdate["new_leadnumber"] = leadNumber;
                                }
                            }
                            service.Update(accountUpdate);
                        }
                    }
                    #endregion
                }
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private EntityReference GetOwnerUser(IOrganizationService service, EntityReference ownerRef)
        {
            try
            {
                if (ownerRef != null && ownerRef.LogicalName == "team")
                {
                    var team = service.Retrieve(ownerRef.LogicalName, ownerRef.Id, new ColumnSet("administratorid"));

                    if (team != null && team.Contains("administratorid") && team["administratorid"] != null)
                    {
                        return (EntityReference)team["administratorid"];
                    }
                }

                return ownerRef;
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private Entity GetCampaignActivity(IOrganizationService service, string v)
        {
            try
            {
                QueryExpression query = new QueryExpression("campaignactivity");
                query.ColumnSet = new ColumnSet("activityid");

                FilterExpression filter = new FilterExpression(LogicalOperator.And);
                filter.AddCondition("new_callist", ConditionOperator.Equal, "ts_self_reged");

                query.Criteria = filter;
                query.NoLock = true;

                DataCollection<Entity> activities = service.RetrieveMultiple(query).Entities;

                return (activities != null && activities.Count > 0) ? activities[0] : null;
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private EntityReference GetList(IOrganizationService service)
        {
            try
            {
                EntityReference listRef = null;
                QueryExpression query = new QueryExpression("new_systemparameter");
                query.ColumnSet = new ColumnSet("new_listid");
                query.NoLock = true;
                query.PageInfo = new PagingInfo() { Count = 1, PageNumber = 1 };

                var systemparameters = service.RetrieveMultiple(query).Entities;

                if (systemparameters != null && systemparameters.Count > 0 && systemparameters[0] != null)
                {
                    listRef = systemparameters[0].GetAttributeValue<EntityReference>("new_listid");
                }

                return listRef;
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private void Update_PreOperation(IPluginExecutionContext context, IOrganizationService service)
        {
            try
            {
                CreateCrmLog(service, System.Reflection.MethodBase.GetCurrentMethod().Name + ": " + context.Stage + " " + context.MessageName);

                Entity targetEntity = (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity) ? (Entity)context.InputParameters["Target"] : null;
                Entity preImageEntity = (context.PreEntityImages != null && context.PreEntityImages.Contains(this.preImageAlias)) ? context.PreEntityImages[this.preImageAlias] : null;

                #region При изменении new_holdingid
                if (targetEntity.Contains("new_holdingid"))
                {
                    EntityReference holdingER = (EntityReference)targetEntity["new_holdingid"];

                    EntityReference ownerER = targetEntity.Contains("ownerid") ? (EntityReference)targetEntity["ownerid"]
                        : preImageEntity.Contains("ownerid") ? (EntityReference)preImageEntity["ownerid"]
                        : null;

                    bool? target = targetEntity.Contains("new_target") ? (bool?)targetEntity["new_target"]
                        : preImageEntity.Contains("new_target") ? (bool?)preImageEntity["new_target"]
                        : (bool?)null;

                    if (target == true)
                    {
                        if (holdingER == null)
                        {
                            targetEntity["new_target"] = false;
                        }
                        else if (ownerER != null)
                        {
                            DataCollection<Entity> holdingAccounts = service.RetrieveMultiple(GetQueryForHoldingAccounts(targetEntity.Id, holdingER.Id, ownerER.Id)).Entities;
                            if (holdingAccounts.Count == 0) targetEntity["new_target"] = false;
                        }
                    }
                }
                #endregion

                #region QualifyLeadRequest
                Entity currentAccount = service.Retrieve(targetEntity.LogicalName, targetEntity.Id,
                    new ColumnSet(
                        "new_terrytoryid",
                        "new_contactforinvoiceid",
                        "originatingleadid",
                        "new_clientnumber"
                    )
                );

                if (targetEntity.Attributes.ContainsKey("new_clientnumber") && targetEntity.Attributes["new_clientnumber"] != null
                     && currentAccount.Attributes.ContainsKey("originatingleadid") && currentAccount.Attributes["originatingleadid"] != null)
                {
                    string newClientnumber = targetEntity.Attributes["new_clientnumber"].ToString();
                    EntityReference originatingleadid = (EntityReference)currentAccount.Attributes["originatingleadid"];

                    if (originatingleadid != null && !string.IsNullOrEmpty(newClientnumber) && !currentAccount.Contains("new_clientnumber"))
                    {
                        Entity lead = service.Retrieve(originatingleadid.LogicalName, originatingleadid.Id, new ColumnSet("statecode"));
                        if (lead != null && lead.Contains("statecode") && lead["statecode"] != null && ((OptionSetValue)lead["statecode"]).Value == 0)
                        {
                            QualifyLeadRequest qualifyLeadReq = new QualifyLeadRequest
                            {
                                CreateAccount = false,
                                CreateContact = false,
                                CreateOpportunity = false,
                                Status = new OptionSetValue(3),
                                LeadId = new EntityReference(lead.LogicalName, lead.Id)
                            };
                            service.Execute(qualifyLeadReq);
                        }
                    }
                }

                if (targetEntity.Attributes.ContainsKey("new_terrytoryid") && targetEntity.Attributes["new_terrytoryid"] != null)
                {
                    Guid newTerrytoryId = ((EntityReference)targetEntity.Attributes["new_terrytoryid"]).Id;
                    if (newTerrytoryId != Guid.Empty
                        && (!currentAccount.Contains("new_terrytoryid") || ((EntityReference)currentAccount["new_terrytoryid"]).Id != newTerrytoryId))
                    {
                        UpdateOportunity(service, targetEntity.Id, newTerrytoryId);
                    }
                }
                #endregion

                #region Назначение холдинга
                List<string> ignore_inns = new List<string>() { "0", "1" };

                string inn = targetEntity.Contains("new_inn") ? targetEntity["new_inn"].ToString()
                            : preImageEntity.Contains("new_inn") ? preImageEntity["new_inn"].ToString()
                            : string.Empty;

                if (!string.IsNullOrEmpty(inn) && !ignore_inns.Contains(inn))
                {
                    string clientnumber = targetEntity.Contains("new_clientnumber") ? targetEntity["new_clientnumber"].ToString() : null;
                    if (clientnumber == null) clientnumber = preImageEntity.Contains("new_clientnumber") ? preImageEntity["new_clientnumber"].ToString() : null;

                    //если у текущего клиента есть клиентский номер
                    if (!string.IsNullOrEmpty(clientnumber))
                    {
                        if (targetEntity.Contains("new_inn"))
                        {
                            if (!string.IsNullOrEmpty(targetEntity["new_inn"].ToString()))
                                CheckInn_SetHolding(service, preImageEntity, targetEntity, targetEntity);
                        }
                        else if (targetEntity.Contains("new_holdingid") && CheckParentContext(context))
                        {
                            inn = (preImageEntity != null && preImageEntity.Contains("new_inn")) ? preImageEntity["new_inn"].ToString() : null;
                            if (inn != null)
                            {
                                EntityReference holding = (preImageEntity != null && preImageEntity.Contains("new_holdingid")) ? (EntityReference)targetEntity["new_holdingid"] : null;
                                if (holding != null)
                                {
                                    Dictionary<string, object> keys_values = new Dictionary<string, object>();
                                    keys_values.Add("new_inn", inn);
                                    keys_values.Add("new_holdingid", holding.Id);
                                    DataCollection<Entity> accountWith_SameHolding = GetAccountCollection(targetEntity.LogicalName,
                                        new ColumnSet("new_holdingid"), keys_values, targetEntity, service);

                                    if (accountWith_SameHolding.Count > 0)
                                        throw new InvalidPluginExecutionException("Клиент не может быть исключен из Холдига.\nВ Холдинге есть другие Клиенты с таким же ИНН");
                                }
                            }
                        }
                        else if (targetEntity.Contains("new_clientnumber"))
                            CheckInn_SetHolding(service, preImageEntity, targetEntity, preImageEntity);
                    }
                }
                #endregion

                #region Логика на поле "Контактное лицо для счета"
                EntityReference contactforinvoiceid_ER = targetEntity.Contains("new_contactforinvoiceid")
                                                     ? (EntityReference)targetEntity["new_contactforinvoiceid"]
                                                     : (preImageEntity.Contains("new_contactforinvoiceid")
                                                     ? (EntityReference)preImageEntity["new_contactforinvoiceid"]
                                                     : null);


                #region Проставление "Контактное лицо для счета" = false у контактов клиента (исключая контакт для счета)
                QueryExpression qContacts = new QueryExpression()
                {
                    EntityName = "contact",
                    ColumnSet = new ColumnSet("contactid", "new_forfinance"),
                    Criteria = new FilterExpression()
                    {
                        FilterOperator = LogicalOperator.And,
                        Conditions =
                            {
                                new ConditionExpression()
                                {
                                    AttributeName = "new_forfinance",
                                    Operator = ConditionOperator.Equal,
                                    Values = { true }
                                },
                                new ConditionExpression()
                                {
                                    AttributeName = "contactid",
                                    Operator = ConditionOperator.NotEqual,
                                    Values = {
                                        (context.ParentContext.MessageName == "Delete" && context.ParentContext.Stage == 30)
                                        ? ((EntityReference)preImageEntity["new_contactforinvoiceid"]).Id
                                        :(
                                            contactforinvoiceid_ER != null
                                            ? contactforinvoiceid_ER.Id
                                            : Guid.Empty
                                         )
                                    }
                                },
                                new ConditionExpression()
                                {
                                    AttributeName = "parentcustomerid",
                                    Operator = ConditionOperator.Equal,
                                    Values = { targetEntity.Id }
                                }
                            }
                    },
                    NoLock = true
                };

                EntityCollection сontacts = service.RetrieveMultiple(qContacts);
                foreach (Entity contact in сontacts.Entities)
                {
                    Entity contactForUpdate = new Entity(contact.LogicalName);
                    contactForUpdate.Id = contact.Id;
                    contactForUpdate["new_forfinance"] = false;
                    service.Update(contactForUpdate);
                }
                #endregion

                #region Проставление "Контактное лицо для счета" = true у контакта для счета
                if (contactforinvoiceid_ER != null)
                {
                    Entity contactforinvoiceid = service.Retrieve(contactforinvoiceid_ER.LogicalName,
                        contactforinvoiceid_ER.Id, new ColumnSet("contactid", "new_forfinance"));

                    if (contactforinvoiceid != null
                        && !(contactforinvoiceid.Contains("new_forfinance") && (bool)contactforinvoiceid["new_forfinance"] == true))
                    {
                        Entity contactForUpdate = new Entity(contactforinvoiceid.LogicalName);
                        contactForUpdate.Id = contactforinvoiceid.Id;
                        contactForUpdate["new_forfinance"] = true;
                        service.Update(contactForUpdate);
                    }
                }
                #endregion

                #endregion


                #region Передача данных по Клиентам в очередь iDPD
                if (targetEntity.Contains("new_clientnumber") || targetEntity.Contains("new_operationstatus")
                        || targetEntity.Contains("new_inn") || targetEntity.Contains("new_fullname")
                        || targetEntity.Contains("new_holdingid") || targetEntity.Contains("new_exception"))
                {
                    var clientnumber = targetEntity.Contains("new_clientnumber")
                                        ? (targetEntity["new_clientnumber"] != null ? targetEntity["new_clientnumber"].ToString() : string.Empty)
                                        : preImageEntity != null && preImageEntity.Contains("new_clientnumber") && preImageEntity["new_clientnumber"] != null
                                            ? preImageEntity["new_clientnumber"].ToString()
                                            : string.Empty;

                    if (!string.IsNullOrEmpty(clientnumber)) { targetEntity["new_insurancestate"] = new OptionSetValue(100_000_000); }
                }
                #endregion
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private bool CheckParentContext(IPluginExecutionContext context)
        {
            try
            {
                return !(context.ParentContext != null && context.ParentContext.ParentContext != null
                            && context.ParentContext.ParentContext.PrimaryEntityName == "new_holding"
                            && context.ParentContext.ParentContext.MessageName == "Update");
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private void Update_PostOperation(IPluginExecutionContext context, IOrganizationService service)
        {
            try
            {
                CreateCrmLog(service, System.Reflection.MethodBase.GetCurrentMethod().Name + ": " + context.Stage + " " + context.MessageName);

                Entity targetEntity = (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity) ? (Entity)context.InputParameters["Target"] : null;
                Entity preImageEntity = (context.PreEntityImages != null && context.PreEntityImages.Contains(this.preImageAlias)) ? context.PreEntityImages[this.preImageAlias] : null;
                Entity postImageEntity = (context.PostEntityImages != null && context.PostEntityImages.Contains(this.postImageAlias)) ? context.PostEntityImages[this.postImageAlias] : null;
                EntityReference holdingER = postImageEntity.Contains("new_holdingid") ? (EntityReference)postImageEntity["new_holdingid"] : null;
                EntityReference ownerER = postImageEntity.Contains("ownerid") ? (EntityReference)postImageEntity["ownerid"] : null;

                #region При изменении new_target на true
                if (targetEntity.Contains("new_target") && (bool)targetEntity["new_target"] == true)
                {
                    if (holdingER != null && ownerER != null)
                    {
                        DataCollection<Entity> holdingAccounts = service.RetrieveMultiple(GetQueryForHoldingAccounts(targetEntity.Id, holdingER.Id, ownerER.Id, false)).Entities;
                        foreach (var account in holdingAccounts)
                        {
                            Entity accountUpdate = new Entity(account.LogicalName);
                            accountUpdate.Id = account.Id;
                            accountUpdate["new_target"] = true;
                            service.Update(accountUpdate);
                        }
                    }
                }
                #endregion

                if (targetEntity.Contains("ownerid") || targetEntity.Contains("new_terrytoryid"))
                {
                    EntityReference ownerER_new = targetEntity.Contains("ownerid") && targetEntity["ownerid"] != null ? (EntityReference)targetEntity["ownerid"] : null;
                    EntityReference terrytoryER_new = targetEntity.Contains("new_terrytoryid") && targetEntity["new_terrytoryid"] != null ? (EntityReference)targetEntity["new_terrytoryid"] : null;

                    if (ownerER_new != null)
                    {
                        QueryExpression query = new QueryExpression("new_discount");
                        query.ColumnSet = new ColumnSet("new_discountid", "ownerid", "new_territoryid");

                        FilterExpression filter = new FilterExpression(LogicalOperator.And);
                        filter.AddCondition(new ConditionExpression("new_state", ConditionOperator.In, new int[] { 100000000, 100000001, 100000002, 100000005, 100000006, 100000008 }));
                        filter.AddCondition("ownerid", ConditionOperator.NotEqual, ownerER_new.Id);
                        filter.AddCondition("new_accountid", ConditionOperator.Equal, targetEntity.Id);

                        query.Criteria = filter;
                        query.NoLock = true;

                        DataCollection<Entity> discounts = service.RetrieveMultiple(query).Entities;

                        foreach (var discount in discounts)
                        {
                            AssignRequest req = new AssignRequest();
                            req.Assignee = ownerER_new;
                            req.Target = discount.ToEntityReference();

                            AssignResponse res = (AssignResponse)service.Execute(req);
                        }
                    }

                    if (terrytoryER_new != null)
                    {
                        QueryExpression query = new QueryExpression("new_discount");
                        query.ColumnSet = new ColumnSet("new_discountid", "ownerid", "new_territoryid");

                        FilterExpression filter = new FilterExpression(LogicalOperator.And);
                        filter.AddCondition(new ConditionExpression("new_state", ConditionOperator.In, new int[] { 100000000, 100000001, 100000002, 100000005, 100000006, 100000008 }));
                        filter.AddCondition("new_territoryid", ConditionOperator.NotEqual, terrytoryER_new.Id);
                        filter.AddCondition("new_accountid", ConditionOperator.Equal, targetEntity.Id);

                        query.Criteria = filter;
                        query.NoLock = true;

                        DataCollection<Entity> discounts = service.RetrieveMultiple(query).Entities;

                        foreach (var discount in discounts)
                        {
                            Entity discountUpdate = new Entity(discount.LogicalName);
                            discountUpdate.Id = discount.Id;
                            discountUpdate["new_territoryid"] = terrytoryER_new;

                            service.Update(discountUpdate);
                        }
                    }
                }

                if (targetEntity.Contains("new_statusid"))
                {
                    var old_statusRef = preImageEntity.GetAttributeValue<EntityReference>("new_statusid");
                    var new_statusRef = targetEntity.GetAttributeValue<EntityReference>("new_statusid");

                    if ((old_statusRef != null && new_statusRef == null)
                        || (old_statusRef == null && new_statusRef != null)
                        || (old_statusRef != null && new_statusRef != null && old_statusRef.Id != new_statusRef.Id))
                    {
                        var historyCreate = new Entity("new_account_history");
                        historyCreate["new_name"] = "Изменение статуса";
                        historyCreate["new_newstatusid"] = new_statusRef;
                        historyCreate["new_oldstatusid"] = old_statusRef;
                        historyCreate["ownerid"] = new EntityReference("systemuser", context.InitiatingUserId);
                        historyCreate["new_accountid"] = targetEntity.ToEntityReference();
                        
                        service.Create(historyCreate);
                    }
                }
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private void Assign_PostOperation(IPluginExecutionContext context, IOrganizationService service)
        {
            try
            {
                CreateCrmLog(service, System.Reflection.MethodBase.GetCurrentMethod().Name + ": " + context.Stage + " " + context.MessageName);

                EntityReference targetER = (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference) ? (EntityReference)context.InputParameters["Target"] : null;
                EntityReference assigneeER = (context.InputParameters.Contains("Assignee") && context.InputParameters["Assignee"] is EntityReference) ? (EntityReference)context.InputParameters["Assignee"] : null;

                if (targetER != null && assigneeER != null)
                {
                    Entity currentAccount = service.Retrieve(targetER.LogicalName, targetER.Id,
                       new ColumnSet(
                           "new_terrytoryid",
                           "new_filialid",
                           "new_regionid",
                           "new_countryid",
                           "new_clientnumber",
                           "ownerid",
                           "new_majoraccount"
                       )
                    );

                    var query = PrepareQuery(assigneeER, currentAccount);

                    EntityCollection searchResult = service.RetrieveMultiple(query);
                    Entity updateAccount = new Entity(currentAccount.LogicalName);
                    updateAccount.Id = currentAccount.Id;
                    bool IsAccountChanged = false;

                    if (searchResult != null && searchResult.Entities.Count > 0)
                    {
                        var territory = searchResult.Entities[0];

                        if (!(currentAccount.Contains("new_terrytoryid") && ((EntityReference)currentAccount["new_terrytoryid"]).Id == territory.Id))
                        {
                            updateAccount["new_terrytoryid"] = new EntityReference("new_territory", territory.Id);
                            IsAccountChanged = true;
                        }
                    }

                    if (!currentAccount.Contains("new_clientnumber"))
                    {
                        var territoryER = (EntityReference)currentAccount["new_terrytoryid"];
                        Entity territory = service.Retrieve(territoryER.LogicalName, territoryER.Id,
                           new ColumnSet(
                               "new_filialid"
                           )
                        );

                        var filialER = (EntityReference)territory["new_filialid"];
                        Entity filial = service.Retrieve(filialER.LogicalName, filialER.Id,
                           new ColumnSet(
                               "new_filialid",
                               "new_regionid",
                               "new_countryid"
                           )
                        );

                        //new_country
                        if (!(currentAccount.Contains("new_filialid") && ((EntityReference)currentAccount["new_filialid"]).Id == filial.Id))
                        {
                            updateAccount["new_filialid"] = new EntityReference("new_filial", filial.Id);
                            IsAccountChanged = true;
                        }

                        if (!(currentAccount.Contains("new_regionid") && filial.Contains("new_regionid")
                              && ((EntityReference)currentAccount["new_regionid"]).Id == ((EntityReference)filial["new_regionid"]).Id))
                        {
                            updateAccount["new_regionid"] = new EntityReference("new_region", ((EntityReference)filial["new_regionid"]).Id);
                            IsAccountChanged = true;
                        }

                        if (!(currentAccount.Contains("new_countryid") && filial.Contains("new_countryid")
                              && ((EntityReference)currentAccount["new_countryid"]).Id == ((EntityReference)filial["new_countryid"]).Id))
                        {
                            updateAccount["new_countryid"] = new EntityReference("new_country", ((EntityReference)filial["new_countryid"]).Id);
                            IsAccountChanged = true;
                        }
                    }

                    UpdateHoldingKurator(service, currentAccount);

                    if (IsAccountChanged)
                    {
                        service.Update(updateAccount);
                    }
                }
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private void Merge_PreValidation(IPluginExecutionContext context, IOrganizationService service)
        {
            try
            {
                CreateCrmLog(service, System.Reflection.MethodBase.GetCurrentMethod().Name + ": " + context.Stage + " " + context.MessageName);

                EntityReference targetER = (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference) ? (EntityReference)context.InputParameters["Target"] : null;
                Guid subordinateId = (context.InputParameters.Contains("SubordinateId") && context.InputParameters["SubordinateId"] is Guid) ? (Guid)context.InputParameters["SubordinateId"] : Guid.Empty;

                if (targetER != null && subordinateId != Guid.Empty)
                {
                    Entity deactivatedAccount = service.Retrieve(targetER.LogicalName, subordinateId,
                       new ColumnSet(
                           "new_clientnumber"
                       )
                    );

                    if (!string.IsNullOrWhiteSpace(deactivatedAccount["new_clientnumber"].ToString())) throw new InvalidPluginExecutionException("Слияние выполнить нельзя");
                }
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        protected QueryExpression GetQueryForHoldingAccounts(Guid accountId, Guid holdingId, Guid ownerId, bool targetEqual = true)
        {
            try
            {
                QueryExpression qAccounts = new QueryExpression()
                {
                    EntityName = "account",
                    ColumnSet = new ColumnSet(
                                        "new_target"
                                    ),
                    Criteria = new FilterExpression()
                    {
                        FilterOperator = LogicalOperator.And,
                        Conditions = {
                            new ConditionExpression()
                            {
                                AttributeName = "statecode",
                                Operator = ConditionOperator.Equal,
                                Values = { 0 }
                            },
                            new ConditionExpression()
                            {
                                AttributeName = "new_holdingid",
                                Operator = ConditionOperator.Equal,
                                Values = { holdingId }
                            },
                            new ConditionExpression()
                            {
                                AttributeName = "ownerid",
                                Operator = ConditionOperator.Equal,
                                Values = { ownerId }
                            },
                            new ConditionExpression()
                            {
                                AttributeName = "new_target",
                                Operator = targetEqual ? ConditionOperator.Equal : ConditionOperator.NotEqual,
                                Values = { true }
                            },
                            new ConditionExpression()
                            {
                                AttributeName = "accountid",
                                Operator = ConditionOperator.NotEqual,
                                Values = { accountId }
                            },
                        }
                    },
                    NoLock = true
                };

                return qAccounts;
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        protected DataCollection<Entity> GetAccountCollection(string logicalName, ColumnSet cols, Dictionary<string, object> keys_values, Entity targetEntity,
            IOrganizationService service)
        {
            try
            {
                QueryExpression query = new QueryExpression(logicalName);
                query.ColumnSet = cols;
                FilterExpression filter = new FilterExpression(LogicalOperator.And);

                foreach (var field in keys_values)
                {
                    if (field.Value.ToString() == "") filter.AddCondition(field.Key, ConditionOperator.Null);
                    else if (field.Value.ToString() == "not null") filter.AddCondition(field.Key, ConditionOperator.NotNull);
                    else filter.AddCondition(field.Key, ConditionOperator.Equal, field.Value);
                }

                filter.AddCondition("accountid", ConditionOperator.NotEqual, targetEntity.Id);
                query.Criteria = filter;
                query.NoLock = true;

                DataCollection<Entity> entityCollection = service.RetrieveMultiple(query).Entities;
                return entityCollection;
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        protected void CheckInn_SetHolding(IOrganizationService service, Entity preImageEntity, Entity targetEntity, Entity evententityforinn)
        {
            try
            {
                #region 1) ищем клиента с таким же инн, у которого есть клиентский номер, и у которого есть холдинг, и id не текущий
                Dictionary<string, object> keys_values1 = new Dictionary<string, object>();
                keys_values1.Add("new_inn", evententityforinn["new_inn"].ToString());
                keys_values1.Add("new_clientnumber", "not null");
                keys_values1.Add("new_holdingid", "not null");

                DataCollection<Entity> accountWith_Inn_ClientNumber_Holding = GetAccountCollection(targetEntity.LogicalName,
                                                                        new ColumnSet("new_holdingid"), keys_values1, targetEntity, service);
                #endregion

                #region 2) ищем клиента с таким же инн, у которого есть клиентский номер, и у которого нету холдинга, и id не текущий
                Dictionary<string, object> keys_values2 = new Dictionary<string, object>();
                keys_values2.Add("new_inn", evententityforinn["new_inn"].ToString());
                keys_values2.Add("new_clientnumber", "not null");
                keys_values2.Add("new_holdingid", "");

                DataCollection<Entity> accountWith_Inn_ClientNumber = GetAccountCollection(targetEntity.LogicalName,
                                                                        new ColumnSet("new_holdingid", "name"), keys_values2, targetEntity, service);
                #endregion

                #region если 1) больше 0 то присваиваем нашему такой же холдинг, затем если 2) больше 0, присваиваем каждому из них этот холдинг
                if (accountWith_Inn_ClientNumber_Holding.Count > 0)
                {
                    EntityReference exist_holding = (EntityReference)accountWith_Inn_ClientNumber_Holding[0]["new_holdingid"];
                    targetEntity["new_holdingid"] = exist_holding;

                    if (accountWith_Inn_ClientNumber.Count > 0)
                        foreach (Entity account in accountWith_Inn_ClientNumber)
                            if (!account.Contains("new_holdingid"))
                            {
                                Entity update_account = new Entity(account.LogicalName);
                                update_account.Id = account.Id;
                                update_account["new_holdingid"] = exist_holding;
                                service.Update(update_account);
                            }
                }
                #endregion

                #region иначе если 2) больше 0 - создаем новый холдинг и присваиваем нашему клиенту и остальным из 2)
                else if (accountWith_Inn_ClientNumber.Count > 0)
                {
                    string accountname = accountWith_Inn_ClientNumber[0].Contains("name") ? accountWith_Inn_ClientNumber[0]["name"].ToString() : "";
                    Entity newholding = CreateHolding(service, accountWith_Inn_ClientNumber[0], accountname);
                    targetEntity["new_holdingid"] = newholding.ToEntityReference();

                    foreach (Entity account in accountWith_Inn_ClientNumber)
                        if (!account.Contains("new_holdingid"))
                        {
                            Entity update_account = new Entity(account.LogicalName);
                            update_account.Id = account.Id;
                            update_account["new_holdingid"] = newholding.ToEntityReference();
                            service.Update(update_account);
                        }
                }
                #endregion

                #region иначе оставляем пустым холдинг у текущего клиента (если нет зарегистрированных клиентов с таким же инн)
                else
                {
                    targetEntity["new_holdingid"] = null;
                }
                #endregion
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        public static Entity CreateHolding(IOrganizationService service, Entity accountentity, string accountname)
        {
            try
            {
                Entity newholding = new Entity("new_holding");
                newholding["new_name"] = accountname;
                newholding["new_maincompanyid"] = accountentity.ToEntityReference();
                newholding.Id = service.Create(newholding);
                return newholding;
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private void UpdateOportunity(IOrganizationService service, Guid accountId, Guid newTerrytotyId)
        {
            try
            {
                //search entity
                QueryExpression query = new QueryExpression();
                query.EntityName = "opportunity";
                query.ColumnSet = new ColumnSet(new[] { "new_territoryid" });

                //condition
                FilterExpression filter = new FilterExpression(LogicalOperator.And);
                filter.AddCondition("customerid", ConditionOperator.Equal, accountId);
                filter.AddCondition("statecode", ConditionOperator.Equal, 0);
                query.Criteria.AddFilter(filter);

                EntityCollection searchResult = service.RetrieveMultiple(query);

                if (searchResult == null)
                    return;

                foreach (var entity in searchResult.Entities)
                {
                    entity["new_territoryid"] = new EntityReference("new_territory", newTerrytotyId);
                    service.Update(entity);
                }
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private void UpdateContact(IOrganizationService service, Guid contactForInvoiceId, bool forfinance)
        {
            try
            {
                var contactUpdate = new Entity("contact", contactForInvoiceId);

                contactUpdate["new_forfinance"] = forfinance;
                service.Update(contactUpdate);
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private QueryBase PrepareQuery(EntityReference assignObject, Entity currentAccount)
        {
            try
            {
                //search entity
                QueryExpression query = new QueryExpression("new_territory");
                query.ColumnSet = new ColumnSet("new_territoryid", "new_filialid");
                query.NoLock = true;

                //Имя территории
                FilterExpression territoryNameFilter = new FilterExpression();

                ConditionExpression cndTerritotyNameRUS = new ConditionExpression();
                cndTerritotyNameRUS.AttributeName = "new_name";
                ConditionExpression cndTerritotyNameENG = new ConditionExpression();
                cndTerritotyNameENG.AttributeName = "new_name";

                if (currentAccount["new_majoraccount"] != null && (bool)currentAccount["new_majoraccount"])
                {
                    territoryNameFilter.FilterOperator = LogicalOperator.Or;

                    cndTerritotyNameRUS.Operator = ConditionOperator.Like;
                    cndTerritotyNameRUS.Values.Add("МА%");
                    cndTerritotyNameENG.Operator = ConditionOperator.Like;
                    cndTerritotyNameENG.Values.Add("MA%");
                }
                else
                {
                    territoryNameFilter.FilterOperator = LogicalOperator.And;

                    cndTerritotyNameRUS.Operator = ConditionOperator.NotLike;
                    cndTerritotyNameRUS.Values.Add("МА%");
                    cndTerritotyNameENG.Operator = ConditionOperator.NotLike;
                    cndTerritotyNameENG.Values.Add("MA%");
                }

                territoryNameFilter.Conditions.Add(cndTerritotyNameRUS);
                territoryNameFilter.Conditions.Add(cndTerritotyNameENG);
                query.Criteria.AddFilter(territoryNameFilter);

                //поле «Филиал»
                ConditionExpression cndTerritotyFillial = new ConditionExpression();
                cndTerritotyFillial.AttributeName = "new_filialid";
                cndTerritotyFillial.Operator = ConditionOperator.Equal;
                cndTerritotyFillial.Values.Add(((EntityReference)currentAccount["new_filialid"]).Id);

                query.Criteria.AddCondition(cndTerritotyFillial);

                //1)linked entity для пользователя new_systemuser_new_territory_kurator; 
                if (assignObject.LogicalName == "systemuser")
                {
                    //linked entity
                    LinkEntity le = new LinkEntity();
                    le.LinkFromEntityName = "new_territory";
                    le.LinkFromAttributeName = "new_territoryid";
                    le.LinkToEntityName = "new_systemuser_new_territory_kurator";
                    le.LinkToAttributeName = "new_territoryid";
                    le.JoinOperator = JoinOperator.Inner;

                    //linked entity condition
                    ConditionExpression cndUserId = new ConditionExpression();
                    cndUserId.AttributeName = "systemuserid";
                    cndUserId.Operator = ConditionOperator.Equal;
                    cndUserId.Values.Add(assignObject.Id);

                    le.LinkCriteria = new FilterExpression();
                    le.LinkCriteria.FilterOperator = LogicalOperator.And;
                    le.LinkCriteria.Conditions.Add(cndUserId);
                    query.LinkEntities.Add(le);
                }

                //2)linked entity для рабочей группы new_team_new_territory.
                if (assignObject.LogicalName == "team")
                {
                    //linked entity
                    LinkEntity le = new LinkEntity();
                    le.LinkFromEntityName = "new_territory";
                    le.LinkFromAttributeName = "new_territoryid";
                    le.LinkToEntityName = "new_team_new_territory";
                    le.LinkToAttributeName = "new_territoryid";
                    le.JoinOperator = JoinOperator.Inner;

                    //linked entity condition
                    ConditionExpression cndTeamId = new ConditionExpression();
                    cndTeamId.AttributeName = "teamid";
                    cndTeamId.Operator = ConditionOperator.Equal;
                    cndTeamId.Values.Add(assignObject.Id);

                    le.LinkCriteria = new FilterExpression();
                    le.LinkCriteria.FilterOperator = LogicalOperator.And;
                    le.LinkCriteria.Conditions.Add(cndTeamId);
                    query.LinkEntities.Add(le);
                }

                return query;
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private void UpdateHoldingKurator(IOrganizationService service, Entity currentAccount)
        {
            try
            {
                //search entity
                QueryExpression query = new QueryExpression();
                query.EntityName = "new_holding";
                query.ColumnSet = new ColumnSet(new[] { "new_holdingid" });
                query.NoLock = true;

                //condition
                ConditionExpression cnd = new ConditionExpression();
                cnd.AttributeName = "new_maincompanyid";
                cnd.Operator = ConditionOperator.Equal;
                cnd.Values.Add(currentAccount.Id);

                query.Criteria.Conditions.Add(cnd);

                EntityCollection searchResult = service.RetrieveMultiple(query);

                if (searchResult != null && searchResult.Entities.Count > 0)
                {
                    var holding = searchResult.Entities[0];

                    AssignRequest request = new AssignRequest();
                    request.Target = new EntityReference(holding.LogicalName, holding.Id);
                    request.Assignee = new EntityReference(((EntityReference)currentAccount["ownerid"]).LogicalName, ((EntityReference)currentAccount["ownerid"]).Id);
                    service.Execute(request);
                }
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        public void CreateCrmLog(IOrganizationService service, string description, int type = 100000001)
        {
            try
            {
                Entity new_integration = new Entity("new_integration");
                new_integration["new_description"] = description;
                new_integration["new_type"] = new OptionSetValue(type);
                new_integration["new_system_source"] = new OptionSetValue(100000000);
                new_integration["new_system_target"] = new OptionSetValue(100000001);
                service.Create(new_integration);
            }
            #region catch
            catch (InvalidPluginExecutionException ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
            catch (Exception ex)
            {
                throw new Exception($"{MethodBase.GetCurrentMethod().Name}: {ex.Message}", ex);
            }
            #endregion
        }

        private Entity GetLeadByLeadNumber(IOrganizationService service, string leadNumber)
        {
            if (!String.IsNullOrEmpty(leadNumber))
            {
                QueryExpression queryLead = new QueryExpression("lead");
                queryLead.NoLock = true;
                queryLead.Criteria.AddCondition(new ConditionExpression("new_id", ConditionOperator.Equal, leadNumber));
                queryLead.ColumnSet = new ColumnSet("ownerid", "new_leadnumber");

                DataCollection<Entity> leads = service.RetrieveMultiple(queryLead).Entities;
                if (leads != null && leads.Count > 0)
                {
                    return leads[0];
                }
            }
            return null;
        }
    }
}
