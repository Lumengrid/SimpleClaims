package com.buuz135.simpleclaims.claim.party;

import com.buuz135.simpleclaims.Main;
import com.buuz135.simpleclaims.claim.ClaimManager;
import com.buuz135.simpleclaims.claim.tracking.ModifiedTracking;
import com.buuz135.simpleclaims.util.Permissions;
import com.hypixel.hytale.server.core.permissions.PermissionsModule;

import javax.annotation.Nullable;
import java.util.*;

public class PartyInfo {

    private UUID id;
    private UUID owner;
    private String name;
    private String description;
    private final Set<UUID> memberSet;
    private int color;
    private final Map<String, PartyOverride> overrideMap;
    private ModifiedTracking createdTracked;
    private ModifiedTracking modifiedTracked;
    private final Set<UUID> partyAllies;
    private final Set<UUID> playerAllies;
    private final Map<UUID, Map<String, Boolean>> permissionOverrides;

    public PartyInfo(UUID id, UUID owner, String name, String description, UUID[] members, int color) {
        this.id = id;
        this.owner = owner;
        this.name = name;
        this.description = description;
        this.memberSet = new HashSet<>();
        this.memberSet.addAll(Arrays.asList(members));
        this.color = color;
        this.overrideMap = new HashMap<>();
        // Don't set default CLAIM_CHUNK_AMOUNT override - calculate dynamically from config/permissions
        setOverride(new PartyOverride(PartyOverrides.PARTY_PROTECTION_PLACE_BLOCKS, new PartyOverride.PartyOverrideValue("bool", Main.CONFIG.get().isDefaultPartyBlockPlaceEnabled())));
        setOverride(new PartyOverride(PartyOverrides.PARTY_PROTECTION_BREAK_BLOCKS, new PartyOverride.PartyOverrideValue("bool", Main.CONFIG.get().isDefaultPartyBlockBreakEnabled())));
        setOverride(new PartyOverride(PartyOverrides.PARTY_PROTECTION_INTERACT, new PartyOverride.PartyOverrideValue("bool", Main.CONFIG.get().isDefaultPartyBlockInteractEnabled())));
        setOverride(new PartyOverride(PartyOverrides.PARTY_PROTECTION_ALLOW_ENTRY, new PartyOverride.PartyOverrideValue("bool", Main.CONFIG.get().isDefaultPartyAllowEntry())));
        setOverride(new PartyOverride(PartyOverrides.PARTY_PROTECTION_INTERACT_PORTAL, new PartyOverride.PartyOverrideValue("bool", Main.CONFIG.get().isDefaultPartyInteractPortal())));
        this.createdTracked = new ModifiedTracking();
        this.modifiedTracked = new ModifiedTracking();
        this.partyAllies = new HashSet<>();
        this.playerAllies = new HashSet<>();
        this.permissionOverrides = new HashMap<>();
    }

    public PartyInfo() {
        this(UUID.randomUUID(), UUID.randomUUID(), "", "", new UUID[0], 0);
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getOwner() {
        return owner;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public UUID[] getMembers() {
        return memberSet.toArray(new UUID[0]);
    }

    public int getTotalMemberCount() {
        return 1 + memberSet.size(); // Owner + members
    }

    public List<UUID> getAllMembers() {
        List<UUID> allMembers = new ArrayList<>();
        allMembers.add(owner);
        allMembers.addAll(memberSet);
        return allMembers;
    }

    public void setOwner(UUID owner) {
        this.owner = owner;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setMembers(UUID[] members) {
        this.memberSet.clear();
        this.memberSet.addAll(Arrays.asList(members));
    }

    public boolean isOwner(UUID uuid){
        return this.owner.equals(uuid);
    }

    public boolean isMember(UUID uuid){
        return memberSet.contains(uuid);
    }

    public boolean isOwnerOrMember(UUID uuid){
        return isOwner(uuid) || isMember(uuid);
    }

    public int getColor() {
        return color;
    }

    public void setColor(int color) {
        this.color = color;
    }

    public List<PartyOverride> getOverrides() {
        return new ArrayList<>(overrideMap.values());
    }

    public void addMember(UUID uuid){
        if (Main.CONFIG.get().getMaxPartyMembers() != -1 && memberSet.size() >= Main.CONFIG.get().getMaxPartyMembers()) {
            return;
        }
        memberSet.add(uuid);
    }

    public void removeMember(UUID uuid){
        memberSet.remove(uuid);
        ClaimManager.getInstance().getPlayerToParty().remove(uuid);
    }

    public int getBaseClaimAmount() {
        // Check for admin base override first
        var baseOverride = this.getOverride(PartyOverrides.CLAIM_CHUNK_BASE);
        if (baseOverride != null) {
            int baseValue = (Integer) baseOverride.getValue().getTypedValue();
            // If scaling enabled, multiply admin override by member count
            if (Main.CONFIG.get().isScaleClaimLimitByMembers()) {
                return baseValue * getTotalMemberCount();
            }
            return baseValue;
        }
        
        // Check for legacy CLAIM_CHUNK_AMOUNT override (backward compatibility)
        var legacyOverride = this.getOverride(PartyOverrides.CLAIM_CHUNK_AMOUNT);
        if (legacyOverride != null) {
            int legacyValue = (Integer) legacyOverride.getValue().getTypedValue();
            // Legacy overrides are treated as absolute (no scaling)
            return legacyValue;
        }
        
        // No override - calculate from permissions/config
        if (!Main.CONFIG.get().isScaleClaimLimitByMembers()) {
            // Legacy mode: only owner's permission or config default
            var amount = Permissions.getPermissionClaimAmount(owner);
            if (amount != -1) {
                return amount;
            }
            return Main.CONFIG.get().getDefaultPartyClaimsAmount();
        } else {
            // Scaling mode: sum all members' permissions or config defaults
            int total = 0;
            for (UUID member : getAllMembers()) {
                var amount = Permissions.getPermissionClaimAmount(member);
                if (amount != -1) {
                    total += amount;
                } else {
                    total += Main.CONFIG.get().getDefaultPartyClaimsAmount();
                }
            }
            return total;
        }
    }

    public int getBonusChunks() {
        var bonusOverride = this.getOverride(PartyOverrides.BONUS_CLAIM_CHUNKS);
        if (bonusOverride != null) {
            return (Integer) bonusOverride.getValue().getTypedValue();
        }
        return 0;
    }

    public int getMaxBonusLimit() {
        if (!Main.CONFIG.get().isScaleClaimLimitByMembers()) {
            // Legacy mode: config max only
            return Main.CONFIG.get().getMaxAddChunkAmount();
        } else {
            // Scaling mode: sum all members' max permissions or config max
            int total = 0;
            for (UUID member : getAllMembers()) {
                var maxAmount = Permissions.getPermissionMaxAddChunkAmount(member);
                if (maxAmount != -1 && maxAmount > Main.CONFIG.get().getMaxAddChunkAmount()) {
                    total += maxAmount;
                } else {
                    total += Main.CONFIG.get().getMaxAddChunkAmount();
                }
            }
            return total;
        }
    }

    public int getMaxClaimAmount(){
        return getBaseClaimAmount() + getBonusChunks();
    }

    public boolean isBlockPlaceEnabled(){
        var override = this.getOverride(PartyOverrides.PARTY_PROTECTION_PLACE_BLOCKS);
        if (override != null) {
            return (Boolean) override.getValue().getTypedValue();
        }
        return Main.CONFIG.get().isDefaultPartyBlockPlaceEnabled();
    }

    public boolean isBlockBreakEnabled(){
        var override = this.getOverride(PartyOverrides.PARTY_PROTECTION_BREAK_BLOCKS);
        if (override != null) {
            return (Boolean) override.getValue().getTypedValue();
        }
        return Main.CONFIG.get().isDefaultPartyBlockBreakEnabled();
    }

    public boolean isBlockInteractEnabled(){
        var override = this.getOverride(PartyOverrides.PARTY_PROTECTION_INTERACT);
        if (override != null) {
            return (Boolean) override.getValue().getTypedValue();
        }
        return Main.CONFIG.get().isDefaultPartyBlockInteractEnabled();
    }

    public boolean isPVPEnabled() {
        var override = this.getOverride(PartyOverrides.PARTY_PROTECTION_PVP);
        if (override != null) {
            return (Boolean) override.getValue().getTypedValue();
        }
        return Main.CONFIG.get().isDefaultPartyPVPEnabled();
    }

    public boolean isFriendlyFireEnabled() {
        var override = this.getOverride(PartyOverrides.PARTY_PROTECTION_FRIENDLY_FIRE);
        if (override != null) {
            return (Boolean) override.getValue().getTypedValue();
        }
        return Main.CONFIG.get().isDefaultPartyFriendlyFireEnabled();
    }

    public boolean isAllowEntryEnabled() {
        var override = this.getOverride(PartyOverrides.PARTY_PROTECTION_ALLOW_ENTRY);
        if (override != null) {
            return (Boolean) override.getValue().getTypedValue();
        }
        return Main.CONFIG.get().isDefaultPartyAllowEntry();
    }

    public boolean isChestInteractEnabled() {
        var override = this.getOverride(PartyOverrides.PARTY_PROTECTION_INTERACT_CHEST);
        if (override != null) {
            return (Boolean) override.getValue().getTypedValue();
        }
        return Main.CONFIG.get().isDefaultPartyInteractChest();
    }

    public boolean isDoorInteractEnabled() {
        var override = this.getOverride(PartyOverrides.PARTY_PROTECTION_INTERACT_DOOR);
        if (override != null) {
            return (Boolean) override.getValue().getTypedValue();
        }
        return Main.CONFIG.get().isDefaultPartyInteractDoor();
    }

    public boolean isBenchInteractEnabled() {
        var override = this.getOverride(PartyOverrides.PARTY_PROTECTION_INTERACT_BENCH);
        if (override != null) {
            return (Boolean) override.getValue().getTypedValue();
        }
        return Main.CONFIG.get().isDefaultPartyInteractBench();
    }

    public boolean isChairInteractEnabled() {
        var override = this.getOverride(PartyOverrides.PARTY_PROTECTION_INTERACT_CHAIR);
        if (override != null) {
            return (Boolean) override.getValue().getTypedValue();
        }
        return Main.CONFIG.get().isDefaultPartyInteractChair();
    }

    public boolean isPortalInteractEnabled() {
        var override = this.getOverride(PartyOverrides.PARTY_PROTECTION_INTERACT_PORTAL);
        if (override != null) {
            return (Boolean) override.getValue().getTypedValue();
        }
        return Main.CONFIG.get().isDefaultPartyInteractPortal();
    }

    public void setOverride(PartyOverride override){
        // Remove override if it matches default value (optimization)
        if (override.getType().equals(PartyOverrides.CLAIM_CHUNK_AMOUNT)
                && (int) override.getValue().tryGetTypedValue().orElse(0) == Main.CONFIG.get().getDefaultPartyClaimsAmount()) {
            overrideMap.remove(override.getType());
            return;
        }
        // Remove CLAIM_CHUNK_BASE if it equals default
        if (override.getType().equals(PartyOverrides.CLAIM_CHUNK_BASE)
                && (int) override.getValue().tryGetTypedValue().orElse(0) == Main.CONFIG.get().getDefaultPartyClaimsAmount()) {
            overrideMap.remove(override.getType());
            return;
        }
        // Remove BONUS_CLAIM_CHUNKS if it's 0
        if (override.getType().equals(PartyOverrides.BONUS_CLAIM_CHUNKS)
                && (int) override.getValue().tryGetTypedValue().orElse(0) == 0) {
            overrideMap.remove(override.getType());
            return;
        }
        overrideMap.put(override.getType(), override);
    }

    public @Nullable PartyOverride getOverride(String type){
        return overrideMap.get(type);
    }

    public void removeOverride(String type) {
        overrideMap.remove(type);
    }

    public ModifiedTracking getCreatedTracked() {
        return createdTracked;
    }

    public void setCreatedTracked(ModifiedTracking createdTracked) {
        this.createdTracked = createdTracked;
    }

    public ModifiedTracking getModifiedTracked() {
        return modifiedTracked;
    }

    public void setModifiedTracked(ModifiedTracking modifiedTracked) {
        this.modifiedTracked = modifiedTracked;
    }

    public Set<UUID> getPartyAllies() {
        return partyAllies;
    }

    public Set<UUID> getPlayerAllies() {
        return playerAllies;
    }

    public void addPartyAllies(UUID uuid) {
        if (Main.CONFIG.get().getMaxPartyAllies() != -1 && (partyAllies.size() + playerAllies.size()) >= Main.CONFIG.get().getMaxPartyAllies()) {
            return;
        }
        partyAllies.add(uuid);
    }

    public void removePartyAllies(UUID uuid) {
        partyAllies.remove(uuid);
    }

    public void addPlayerAllies(UUID uuid) {
        if (Main.CONFIG.get().getMaxPartyAllies() != -1 && (partyAllies.size() + playerAllies.size()) >= Main.CONFIG.get().getMaxPartyAllies()) {
            return;
        }
        playerAllies.add(uuid);
    }

    public void removePlayerAllies(UUID uuid) {
        playerAllies.remove(uuid);
    }

    public boolean isPlayerAllied(UUID uuid) {
        return playerAllies.contains(uuid);
    }

    public boolean isPartyAllied(UUID uuid) {
        return partyAllies.contains(uuid);
    }

    public Map<UUID, Map<String, Boolean>> getPermissionOverrides() {
        return permissionOverrides;
    }

    public boolean hasPermission(UUID uuid, String permission) {
        if (isOwner(uuid)) return true;
        if (isMember(uuid) || partyAllies.contains(uuid) || playerAllies.contains(uuid)) {
            if (permissionOverrides.containsKey(uuid)) {
                Map<String, Boolean> perms = permissionOverrides.get(uuid);
                if (perms.containsKey(permission)) {
                    return perms.get(permission);
                }
            }
        }
        // Default permissions for members
        if (isMember(uuid)) {
            if (permission.equals(PartyOverrides.PARTY_PROTECTION_MODIFY_INFO)) return false;
            return true;
        }
        // Defaul Perm for Allies
        if (playerAllies.contains(uuid) || partyAllies.contains(uuid)) {
            // Allies might have some permissions by default or none. Assuming none for now if not overridden.
            return false;
        }
        return false;
    }

    public boolean hasPartyPermission(UUID partyId, String permission) {
        if (permissionOverrides.containsKey(partyId)) {
            Map<String, Boolean> perms = permissionOverrides.get(partyId);
            if (perms.containsKey(permission)) {
                return perms.get(permission);
            }
        }
        return false;
    }

    public void setPermission(UUID uuid, String permission, boolean value) {
        permissionOverrides.computeIfAbsent(uuid, k -> new HashMap<>()).put(permission, value);
    }

    public void removePermission(UUID uuid, String permission) {
        if (permissionOverrides.containsKey(uuid)) {
            permissionOverrides.get(uuid).remove(permission);
            if (permissionOverrides.get(uuid).isEmpty()) {
                permissionOverrides.remove(uuid);
            }
        }
    }

    @Override
    public String toString() {
        return "PartyInfo{" +
                "id=" + id +
                ", owner=" + owner +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", members=" + memberSet +
                ", color=" + color +
                ", overrides=" + overrideMap.values() +
                ", createdTracked=" + createdTracked +
                ", modifiedTracked=" + modifiedTracked +
                '}';
    }
}
